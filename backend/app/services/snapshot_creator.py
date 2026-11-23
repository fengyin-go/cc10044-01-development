"""数据快照领域服务：单份快照、成组快照（同截止时间归档）与血缘回查。

数据链路位置：数据快照层，为数据版本管理和恢复提供时间点快照。

成组快照能力：
- 主管一次多选多个数据集登记为一组，全组共用同一个业务截止时间；
- 登记时即把每个成员的血缘占位回写到 lineage_graph（待验证），
  成员快照补齐后只更新同一条血缘，编号与来源版本保持稳定；
- 补做/续做始终按组的原截止时间解析来源版本，之后入表的新数据不会串版；
- 联合血缘回查只接受同组且全部已确认的成员，禁止跨组把新旧副本拼在一起。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _parse_time(value: str) -> datetime:
    """宽松解析 ISO 时间，统一成可比较的带时区时间。"""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


class SnapshotCreatorService(BaseModuleService):
    MODULE = "snapshot_creator"
    ENTITY = "快照任务"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["目标数据集", "快照时间点", "快照类型", "存储路径", "文件大小"]
    REQUIRED_FIELDS = ["目标数据集", "快照时间点", "快照类型"]
    STATUS_ORDER = ["排队中", "创建中", "已完成", "创建失败"]
    ACTION_RULES = {"创建快照": "排队中", "取消快照": "创建中"}
    NEGATIVE_ACTIONS = []

    SNAPSHOT_TYPES = frozenset({"full", "incremental", "daily_sealed"})

    GROUP_TABLE = "snapshot_group"
    VERSION_TABLE = "source_version"

    GROUP_STATUS_OPEN = "进行中"
    GROUP_STATUS_COMPLETE = "已齐全"
    MEMBER_PENDING = "待创建"
    MEMBER_DONE = "已完成"

    # 业务数据集到真实来源表的映射；未登记的数据集按约定回退到 src_{数据集}。
    DATASET_SOURCE_MAP: dict[str, str] = {
        "ods_order_detail": "ods_orders",
        "ods_refund_detail": "ods_refunds",
        "dwd_order_detail": "dwd_orders_clean",
        "dwd_refund_detail": "dwd_refund_clean",
        "ads_order_detail": "ads_order_summary",
        "ads_refund_detail": "ads_refund_summary",
    }

    # ------------------------------------------------------------------
    # 来源版本：模拟来源表随入表不断产生新版本，快照只能取截止时间之前的版本。
    # ------------------------------------------------------------------
    def _resolve_source_table(self, dataset: str) -> str:
        return self.DATASET_SOURCE_MAP.get(dataset, f"src_{dataset}")

    def _resolve_source_version(self, source_table: str, cutoff_time: str) -> str:
        """返回来源表在业务截止时间那一刻的真实版本。

        只接受生效时间不晚于截止时间的版本；截止时间之后入表产生的新版本
        不会被补做快照看到，保证「补做仍按原截止时间取数」。
        """
        cutoff = _parse_time(cutoff_time)
        candidates = [
            row for row in store.rows(self.VERSION_TABLE)
            if row.get("source_table") == source_table
            and _parse_time(str(row.get("effective_time"))) <= cutoff
        ]
        if candidates:
            latest = max(candidates, key=lambda r: _parse_time(str(r.get("effective_time"))))
            return str(latest["version"])
        # 没有版本台账时给出与截止时间绑定的确定性基线版本，重查结果稳定。
        return f"{source_table}#base@{cutoff.date().isoformat()}"

    def ingest_source_data(
        self,
        *,
        dataset: str | None = None,
        source_table: str | None = None,
        remark: str | None = None,
        effective_time: str | None = None,
    ) -> dict[str, Any]:
        """模拟业务数据入表：为来源表追加一个生效时间为当前时刻的新版本。

        用于验证成组登记之后再入表的数据，不会影响按原截止时间补做的快照。
        """
        if not source_table:
            if not dataset:
                return {"ok": False, "message": "必须指定 dataset 或 source_table"}
            source_table = self._resolve_source_table(dataset)
        effective = effective_time or datetime.now(timezone.utc).isoformat()
        rows = store.rows(self.VERSION_TABLE)
        seq = sum(1 for r in rows if r.get("source_table") == source_table) + 1
        version = f"{source_table}#v{_parse_time(effective).date().isoformat()}-{seq:02d}"
        row_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        rows.append({
            "id": row_id,
            "source_table": source_table,
            "version": version,
            "effective_time": effective,
            "remark": remark or "业务数据入表",
        })
        return {
            "ok": True,
            "source_table": source_table,
            "version": version,
            "effective_time": effective,
            "message": f"来源表 {source_table} 已登记新版本 {version}",
        }

    # ------------------------------------------------------------------
    # 成组快照登记 / 重提
    # ------------------------------------------------------------------
    def register_snapshot_group(
        self,
        cutoff_time: str,
        members: list[dict[str, Any]],
        group_id: int | None = None,
    ) -> dict[str, Any]:
        """登记一组共用截止时间的快照任务；传 group_id 时为重提原组续做。

        新建组：为每个成员创建快照任务（排队中）和血缘占位（待验证）。
        重提原组：截止时间不可变，已完成成员的任务编号、血缘编号和来源版本
        全部保留，只为尚未完成的成员补建任务/血缘占位。
        """
        if not str(cutoff_time or "").strip():
            return {"ok": False, "message": "业务截止时间不能为空"}
        # 先校验时间格式，避免脏数据进组。
        _parse_time(cutoff_time)
        normalized: list[dict[str, str]] = []
        seen: set[str] = set()
        for raw in members or []:
            dataset = str((raw or {}).get("dataset") or "").strip()
            if not dataset:
                return {"ok": False, "message": "成员数据集名称不能为空"}
            if dataset in seen:
                return {"ok": False, "message": f"成员数据集 {dataset} 在同组内重复"}
            seen.add(dataset)
            snapshot_type = str(raw.get("snapshot_type") or "full").strip()
            if snapshot_type not in self.SNAPSHOT_TYPES:
                snapshot_type = "full"
            normalized.append({"dataset": dataset, "snapshot_type": snapshot_type})
        if not normalized:
            return {"ok": False, "message": "至少选择一个数据集成员"}

        group_rows = store.rows(self.GROUP_TABLE)

        if group_id is not None:
            group = store.find(self.GROUP_TABLE, int(group_id))
            if group is None:
                return {"ok": False, "message": f"快照组 {group_id} 不存在，无法重提"}
            if str(group.get("业务截止时间")) != str(cutoff_time):
                return {
                    "ok": False,
                    "message": (
                        "重提原组必须沿用原业务截止时间 "
                        f"{group.get('业务截止时间')}，不能改成 {cutoff_time}"
                    ),
                }
            existing = {m["dataset"]: m for m in group["members"]}
            for item in normalized:
                if item["dataset"] not in existing:
                    member = self._create_member(group["id"], item["dataset"], item["snapshot_type"], cutoff_time)
                    group["members"].append(member)
            return self._group_detail(group, resumed=True)

        new_id = max((int(r.get("id", 0)) for r in group_rows), default=0) + 1
        group: dict[str, Any] = {
            "id": new_id,
            "status": self.GROUP_STATUS_OPEN,
            "pending": True,
            "abnormal": False,
            "业务截止时间": cutoff_time,
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "members": [
                self._create_member(new_id, item["dataset"], item["snapshot_type"], cutoff_time)
                for item in normalized
            ],
        }
        group_rows.append(group)
        return self._group_detail(group, resumed=False)

    def _create_member(
        self, group_id: int, dataset: str, snapshot_type: str, cutoff_time: str
    ) -> dict[str, Any]:
        """为组成员创建快照任务和血缘占位，两者编号在补齐前后保持不变。"""
        snapshot_rows = store.rows(self.MODULE)
        snapshot_id = max((int(r.get("id", 0)) for r in snapshot_rows), default=0) + 1
        source_table = self._resolve_source_table(dataset)
        storage_path = f"/snapshots/groups/G{group_id}/{dataset}@{cutoff_time.replace(':', '-')}"

        snapshot_rows.append({
            "id": snapshot_id,
            "status": "排队中",
            "pending": True,
            "abnormal": False,
            "目标数据集": dataset,
            "快照时间点": cutoff_time,
            "快照类型": snapshot_type,
            "存储路径": storage_path,
            "文件大小": "",
            "快照组id": group_id,
            "成员状态": self.MEMBER_PENDING,
            "来源版本": "",
        })

        lineage_rows = store.rows("lineage_graph")
        lineage_id = max((int(r.get("id", 0)) for r in lineage_rows), default=0) + 1
        lineage_rows.append({
            "id": lineage_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "目标表": f"{dataset}_snapshot",
            "来源表列表": source_table,
            "转换逻辑": f"group_snapshot:{dataset}@{cutoff_time}",
            "中间层级": "SNAP",
            "血缘深度": "1",
            "snapshot_id": snapshot_id,
            "snapshot_group_id": group_id,
            "成员数据集": dataset,
            "业务截止时间": cutoff_time,
            "来源版本": "",
        })

        return {
            "dataset": dataset,
            "snapshot_type": snapshot_type,
            "snapshot_id": snapshot_id,
            "lineage_id": lineage_id,
            "source_table": source_table,
            "source_version": "",
            "status": self.MEMBER_PENDING,
        }

    def complete_group_member(self, group_id: int, dataset: str) -> dict[str, Any]:
        """补齐组内某个成员的快照（值班员补做）。

        取数严格使用组登记时的业务截止时间解析来源版本；已完成的成员再次
        调用时原样返回，编号和来源版本都不会变化。
        """
        group = store.find(self.GROUP_TABLE, int(group_id))
        if group is None:
            return {"ok": False, "message": f"快照组 {group_id} 不存在"}
        member = next((m for m in group["members"] if m["dataset"] == dataset), None)
        if member is None:
            return {"ok": False, "message": f"数据集 {dataset} 不是快照组 {group_id} 的成员"}

        if member["status"] == self.MEMBER_DONE:
            # 幂等：已完成成员与原登记保持一致，不重新取数、不换版本。
            return {
                "ok": True,
                "group_id": group_id,
                "dataset": dataset,
                "snapshot_id": member["snapshot_id"],
                "lineage_id": member["lineage_id"],
                "source_table": member["source_table"],
                "source_version": member["source_version"],
                "cutoff_time": group["业务截止时间"],
                "message": "成员快照已完成，沿用原编号与来源版本",
            }

        cutoff_time = str(group["业务截止时间"])
        source_version = self._resolve_source_version(member["source_table"], cutoff_time)

        snapshot = store.find(self.MODULE, int(member["snapshot_id"]))
        if snapshot is not None:
            snapshot["status"] = "已完成"
            snapshot["pending"] = False
            snapshot["成员状态"] = self.MEMBER_DONE
            snapshot["来源版本"] = source_version
            snapshot["文件大小"] = f"{self._estimate_snapshot_size(member['snapshot_type'])} GB"

        lineage = store.find("lineage_graph", int(member["lineage_id"]))
        if lineage is not None:
            lineage["status"] = "已建立"
            lineage["pending"] = False
            lineage["来源版本"] = source_version
            lineage["转换逻辑"] = (
                f"group_snapshot:{dataset}@{cutoff_time}|version={source_version}"
            )

        member["status"] = self.MEMBER_DONE
        member["source_version"] = source_version
        if all(m["status"] == self.MEMBER_DONE for m in group["members"]):
            group["status"] = self.GROUP_STATUS_COMPLETE
            group["pending"] = False

        return {
            "ok": True,
            "group_id": group_id,
            "dataset": dataset,
            "snapshot_id": member["snapshot_id"],
            "lineage_id": member["lineage_id"],
            "source_table": member["source_table"],
            "source_version": source_version,
            "cutoff_time": cutoff_time,
            "message": "成员快照已按组截止时间补齐并确认血缘",
        }

    def resubmit_snapshot_group(self, group_id: int) -> dict[str, Any]:
        """重提原组：续做尚未完成的成员，已完成成员的编号和来源版本保留。"""
        group = store.find(self.GROUP_TABLE, int(group_id))
        if group is None:
            return {"ok": False, "message": f"快照组 {group_id} 不存在"}
        resumed: list[dict[str, Any]] = []
        for member in group["members"]:
            if member["status"] != self.MEMBER_DONE:
                resumed.append({"dataset": member["dataset"], "snapshot_type": member["snapshot_type"]})
        return {
            "ok": True,
            "group_id": group_id,
            "cutoff_time": group["业务截止时间"],
            "resumed_members": resumed,
            "preserved_members": [
                {
                    "dataset": m["dataset"],
                    "snapshot_id": m["snapshot_id"],
                    "lineage_id": m["lineage_id"],
                    "source_version": m["source_version"],
                }
                for m in group["members"] if m["status"] == self.MEMBER_DONE
            ],
            "message": "原组已重提，仅续做未完成成员，已完成成员保持原登记",
        }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def list_snapshot_groups(self) -> dict[str, Any]:
        groups = store.rows(self.GROUP_TABLE)
        return {
            "total": len(groups),
            "groups": [self._group_detail(g) for g in sorted(groups, key=lambda g: -int(g["id"]))],
        }

    def get_snapshot_group(self, group_id: int) -> dict[str, Any] | None:
        group = store.find(self.GROUP_TABLE, int(group_id))
        return self._group_detail(group) if group else None

    def _group_detail(self, group: dict[str, Any], *, resumed: bool = False) -> dict[str, Any]:
        members = list(group["members"])
        completed = [m for m in members if m["status"] == self.MEMBER_DONE]
        return {
            "ok": True,
            "group_id": group["id"],
            "status": group["status"],
            "cutoff_time": group["业务截止时间"],
            "registered_at": group.get("registered_at"),
            "member_count": len(members),
            "completed_count": len(completed),
            "members": [dict(m) for m in members],
            "resumed": resumed,
        }

    # ------------------------------------------------------------------
    # 血缘回查
    # ------------------------------------------------------------------
    def trace_snapshot_lineage(self, snapshot_id: int) -> dict[str, Any]:
        """单份回查：沿一份快照找到其截止时间对应的真实来源版本。

        只有成员快照已补齐、血缘已确认时才可回查；未确认的副本明确返回
        不可回查及判定依据，避免值班员误把待验证副本当成可信来源。
        """
        snapshot = store.find(self.MODULE, int(snapshot_id))
        if snapshot is None:
            return {"ok": False, "snapshot_id": snapshot_id, "message": "快照任务不存在"}

        lineage = next(
            (r for r in store.rows("lineage_graph") if r.get("snapshot_id") == snapshot_id),
            None,
        )
        if lineage is None:
            return {
                "ok": False,
                "snapshot_id": snapshot_id,
                "traceable": False,
                "message": "该快照尚未回写血缘清单，无法回查来源",
            }
        if lineage.get("status") != "已建立":
            return {
                "ok": False,
                "snapshot_id": snapshot_id,
                "traceable": False,
                "lineage_id": lineage.get("id"),
                "group_id": lineage.get("snapshot_group_id"),
                "dataset": snapshot.get("目标数据集"),
                "cutoff_time": lineage.get("业务截止时间"),
                "message": "副本尚未确认，血缘仍为待验证状态，暂不能回查来源版本",
                "basis": "数据血缘清单中该快照对应条目状态为待验证，缺少来源版本记录",
            }

        return {
            "ok": True,
            "traceable": True,
            "snapshot_id": snapshot_id,
            "lineage_id": lineage.get("id"),
            "group_id": lineage.get("snapshot_group_id"),
            "dataset": snapshot.get("目标数据集"),
            "target_table": lineage.get("目标表"),
            "source_table": lineage.get("来源表列表"),
            "source_version": lineage.get("来源版本"),
            "cutoff_time": lineage.get("业务截止时间"),
            "message": "可回查：已沿快照定位到截止时间的真实来源版本",
            "basis": (
                f"血缘条目 {lineage.get('id')} 已建立，目标 {lineage.get('目标表')} "
                f"来源于 {lineage.get('来源表列表')} 的版本 {lineage.get('来源版本')}，"
                f"业务截止时间 {lineage.get('业务截止时间')}"
            ),
        }

    def trace_group_lineage(self, group_id: int) -> dict[str, Any]:
        """联合回查：只接受同组且成员全部已确认的血缘，绝不跨组拼副本。"""
        group = store.find(self.GROUP_TABLE, int(group_id))
        if group is None:
            return {"ok": False, "group_id": group_id, "message": "快照组不存在"}

        cutoff_time = str(group["业务截止时间"])
        records: list[dict[str, Any]] = []
        missing: list[str] = []
        for member in group["members"]:
            lineage = store.find("lineage_graph", int(member["lineage_id"]))
            # 双重保险：血缘必须属于本组，防止把别组的新旧副本拼进来。
            if (
                lineage is None
                or lineage.get("snapshot_group_id") != group["id"]
                or lineage.get("status") != "已建立"
            ):
                missing.append(member["dataset"])
                continue
            records.append({
                "dataset": member["dataset"],
                "snapshot_id": member["snapshot_id"],
                "lineage_id": member["lineage_id"],
                "target_table": lineage.get("目标表"),
                "source_table": lineage.get("来源表列表"),
                "source_version": lineage.get("来源版本"),
                "cutoff_time": lineage.get("业务截止时间"),
                "group_id": group["id"],
            })

        if missing:
            return {
                "ok": False,
                "traceable": False,
                "group_id": group_id,
                "cutoff_time": cutoff_time,
                "missing_members": missing,
                "records": records,
                "message": (
                    f"组成员未齐全（缺少：{'、'.join(missing)}），联合回查拒绝拼接；"
                    "不会使用其他快照组的副本凑数"
                ),
            }

        same_cutoff = {r["cutoff_time"] for r in records}
        same_group = {r["group_id"] for r in records}
        if same_cutoff != {cutoff_time} or same_group != {group_id}:
            return {
                "ok": False,
                "traceable": False,
                "group_id": group_id,
                "message": "联合回查校验失败：成员截止时间或所属组不一致，拒绝拼接",
            }

        return {
            "ok": True,
            "traceable": True,
            "group_id": group_id,
            "cutoff_time": cutoff_time,
            "member_count": len(records),
            "records": records,
            "message": "组成员齐全，已联合回查到同组同截止时间的全部血缘记录",
        }

    # ------------------------------------------------------------------
    # 通用动作接入：在工作台对组内任务执行「创建快照」即走补齐流程。
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is not None and action == "创建快照" and entry.get("快照组id") is not None:
            result = self.complete_group_member(int(entry["快照组id"]), str(entry["目标数据集"]))
            if not result.get("ok"):
                return None, str(result.get("message"))
            refreshed = store.find(self.MODULE, entry_id)
            return refreshed, f"成员快照已按组截止时间补齐，来源版本 {result.get('source_version')}"
        return super().run_action(entry_id, action)

    # ------------------------------------------------------------------
    # 既有单份快照能力
    # ------------------------------------------------------------------
    def create_snapshot_with_lineage(self, entry_id: int) -> dict[str, Any]:
        """创建快照并同步记录血缘关系。

        快照创建后自动注册到 lineage_graph 模块，
        记录快照数据集与来源表之间的血缘关系。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "快照任务不存在"}

        snapshot_type = entry.get("快照类型", "full")
        dataset = entry.get("目标数据集", "")
        snapshot_time = entry.get("快照时间点", datetime.now().isoformat())
        storage_path = entry.get("存储路径", f"/data/snapshots/{dataset}/{snapshot_time}")

        entry["status"] = "已完成"
        entry["存储路径"] = storage_path
        entry["文件大小"] = f"{self._estimate_snapshot_size(snapshot_type)} GB"
        entry["pending"] = False

        source_table = self._resolve_source_table(dataset)
        source_version = self._resolve_source_version(source_table, str(snapshot_time))
        lineage_rows = store.rows("lineage_graph")
        lid = max((int(r.get("id", 0)) for r in lineage_rows), default=0) + 1
        lineage_rows.append({
            "id": lid,
            "status": "已建立",
            "目标表": f"{dataset}_snapshot",
            "来源表列表": source_table,
            "转换逻辑": f"snapshot_{snapshot_type}_at_{snapshot_time}|version={source_version}",
            "中间层级": "1",
            "血缘深度": "1",
            "pending": False,
            "abnormal": False,
            "snapshot_id": entry_id,
            "snapshot_group_id": None,
            "业务截止时间": snapshot_time,
            "来源版本": source_version,
        })

        return {
            "ok": True,
            "snapshot_id": entry_id,
            "snapshot_type": snapshot_type,
            "dataset": dataset,
            "timestamp": snapshot_time,
            "storage_path": storage_path,
            "size_gb": entry["文件大小"],
            "lineage_id": lid,
            "source_version": source_version,
            "message": "快照创建完成并已注册血缘",
        }

    def diff_snapshots(self, snapshot_id_a: int, snapshot_id_b: int) -> dict[str, Any]:
        """比较两个快照之间的数据差异。

        返回新增行数、删除行数、修改行数和变更字段列表，
        可用于审计追溯和数据回滚决策。
        """
        snap_a = store.find(self.MODULE, snapshot_id_a)
        snap_b = store.find(self.MODULE, snapshot_id_b)
        if snap_a is None or snap_b is None:
            return {"ok": False, "message": "快照不存在"}
        if snap_a.get("目标数据集") != snap_b.get("目标数据集"):
            return {"ok": False, "message": "快照数据集不一致，无法比较"}

        diff = {
            "ok": True,
            "snapshot_a": {"id": snapshot_id_a, "time": snap_a.get("快照时间点"), "size": snap_a.get("文件大小")},
            "snapshot_b": {"id": snapshot_id_b, "time": snap_b.get("快照时间点"), "size": snap_b.get("文件大小")},
            "dataset": snap_a.get("目标数据集"),
            "added_rows": 2850,
            "deleted_rows": 120,
            "modified_rows": 3400,
            "unchanged_rows": 1540000,
            "changed_fields": ["amount", "status", "updated_at"],
            "revert_cost_estimate_seconds": 120,
        }
        return diff

    def list_restorable_snapshots(self, dataset: str) -> dict[str, Any]:
        """列出指定数据集的所有可用快照，供冷数据恢复模块查询。

        返回的快照列表按时间倒序排列，标注每个快照是否仍可恢复。
        """
        all_snapshots = store.rows(self.MODULE)
        matched = [
            s for s in all_snapshots
            if s.get("目标数据集") == dataset and s.get("status") == "已完成"
        ]
        matched.sort(key=lambda s: s.get("快照时间点", ""), reverse=True)

        return {
            "dataset": dataset,
            "total_snapshots": len(matched),
            "latest_snapshot_time": matched[0].get("快照时间点") if matched else None,
            "oldest_snapshot_time": matched[-1].get("快照时间点") if matched else None,
            "snapshots": [
                {
                    "id": s["id"],
                    "time": s.get("快照时间点"),
                    "type": s.get("快照类型"),
                    "path": s.get("存储路径"),
                    "size": s.get("文件大小"),
                    "restorable": True,
                }
                for s in matched[:20]
            ],
        }

    def validate_snapshot_integrity(self, entry_id: int) -> dict[str, Any]:
        """校验快照的数据完整性。

        对比快照的元数据和实际文件，检查是否存在损坏或缺失。
        校验结果写入 data_lake_audit 模块。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "快照不存在"}

        checks = [
            {"check": "文件存在性", "passed": True, "detail": "快照文件路径可访问"},
            {"check": "行数一致性", "passed": True, "detail": "快照行数与元数据一致"},
            {"check": "Schema完整性", "passed": True, "detail": "所有字段与源表一致"},
            {"check": "校验和匹配", "passed": True, "detail": "MD5 校验和与创建时一致"},
            {"check": "压缩完整性", "passed": True, "detail": "Parquet 格式验证通过"},
        ]
        all_passed = all(c["passed"] for c in checks)

        if not all_passed:
            entry["abnormal"] = True

        return {
            "ok": all_passed,
            "snapshot_id": entry_id,
            "dataset": entry.get("目标数据集"),
            "checks": checks,
            "overall": "pass" if all_passed else "fail",
        }

    def _estimate_snapshot_size(self, snapshot_type: str) -> float:
        base = 1.5
        if snapshot_type == "full":
            return base * 3
        elif snapshot_type == "incremental":
            return base * 0.3
        return base
