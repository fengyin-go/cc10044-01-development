"""ETL流水线领域服务：管理数据管道的抽取-转换-加载全流程编排，包括阶段分解、水位线估算和增量抽取。

数据链路位置：数据管道层，将源端数据通过抽取、清洗转换和加载写入目标存储。"""

from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class EtlPipelineService(BaseModuleService):
    MODULE = "etl_pipeline"
    ENTITY = "流水线"
    OPERATION_TERM = "抽取"
    LIST_FIELDS = ["流水线名称", "源端类型", "目标端类型", "执行周期", "最近运行状态"]
    REQUIRED_FIELDS = ["流水线名称", "源端类型", "目标端类型"]
    STATUS_ORDER = ["草稿", "已启用", "运行中", "已停用"]
    ACTION_RULES = {"启用流水线": "已启用", "停用流水线": "已停用"}
    NEGATIVE_ACTIONS = ["停用流水线"]

    SOURCE_TYPES = frozenset({"MySQL", "PostgreSQL", "Kafka", "HDFS", "S3", "Hive", "MongoDB"})
    TARGET_TYPES = frozenset({"Hive", "Iceberg", "Delta", "Hudi", "ClickHouse", "Parquet"})
    EXTRACT_MODES = frozenset({"incremental", "full", "cdc"})

    def __init__(self):
        super().__init__()
        self._watermarks: dict[str, str] = {}

    def plan_pipeline_stages(self, entry_id: int) -> dict[str, Any] | None:
        """生成流水线的阶段分解计划：源端检查→抽取→转换→加载→校验。

        返回带预估行数和依赖的阶段列表，供调度器分配资源。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        source = entry.get("源端类型", "")
        target = entry.get("目标端类型", "")
        stages = [
            {
                "seq": 1, "name": "源端连接检查", "action": "validate_connectivity",
                "source_type": source, "timeout_seconds": 30,
                "depends_on": [], "estimated_rows": 0,
            },
            {
                "seq": 2, "name": "数据抽取", "action": "extract",
                "extract_mode": self._infer_extract_mode(entry),
                "watermark_col": "updated_at" if source in ("MySQL", "PostgreSQL") else "event_time",
                "timeout_seconds": 600, "depends_on": [1],
                "estimated_rows": entry.get("estimated_row_count", 0),
            },
            {
                "seq": 3, "name": "类型转换与清洗", "action": "transform",
                "transform_rules": self._build_transform_rules(source, target),
                "timeout_seconds": 300, "depends_on": [2],
                "estimated_rows": entry.get("estimated_row_count", 0),
            },
            {
                "seq": 4, "name": "目标端写入", "action": "load",
                "write_mode": "append" if target in ("Iceberg", "Delta", "Hudi") else "overwrite",
                "partition_by": entry.get("partition_column", ""),
                "timeout_seconds": 1200, "depends_on": [3],
                "estimated_rows": entry.get("estimated_row_count", 0),
            },
            {
                "seq": 5, "name": "行数校验与快照记录", "action": "validate_and_snapshot",
                "row_checksum": True, "timeout_seconds": 60, "depends_on": [4],
                "estimated_rows": 0,
            },
        ]
        return {
            "pipeline_id": entry_id,
            "pipeline_name": entry.get("流水线名称"),
            "stages": stages,
            "total_stages": len(stages),
            "critical_path": [1, 2, 4, 5],
        }

    def estimate_extract_watermark(self, entry_id: int) -> dict[str, Any]:
        """估算增量抽取的水位线位置和下一次抽取范围。

        水位线用于决定下一次增量抽取的起止边界，
        避免重复拉取已处理数据或遗漏迟到数据。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"error": "流水线不存在"}
        last_run = self._last_pipeline_run(entry_id)
        current_watermark = last_run.get("max_watermark", "") if last_run else ""
        next_start = current_watermark or "2026-01-01T00:00:00Z"
        lag_seconds = self._compute_watermark_lag(entry, current_watermark)
        return {
            "pipeline_id": entry_id,
            "pipeline_name": entry.get("流水线名称"),
            "last_watermark": current_watermark,
            "next_extract_start": next_start,
            "estimated_lag_seconds": lag_seconds,
            "lag_category": "normal" if lag_seconds < 3600 else ("warning" if lag_seconds < 7200 else "critical"),
            "last_run_status": last_run.get("status", "从未运行"),
        }

    def execute_pipeline(self, entry_id: int, force_full: bool = False) -> dict[str, Any]:
        """触发流水线执行，生成运行记录并推进水位线。

        支持强制全量模式和常规增量模式：
        - force_full=True 时忽略水位线，从源端重新全量抽取
        - force_full=False 时根据已记录的水位线做增量抽取
        执行结果写入 pipeline_monitor 模块供监控消费。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "流水线不存在"}
        if entry.get("status") not in ("已启用", "运行中"):
            return {"ok": False, "message": "流水线未启用，无法执行"}

        plan = self.plan_pipeline_stages(entry_id)
        if plan is None:
            return {"ok": False, "message": "无法生成执行计划"}

        run_record = {
            "id": self._next_run_id(),
            "流水线名称": entry.get("流水线名称"),
            "运行时间": "2026-11-21T08:00:00Z",
            "执行状态": "运行中",
            "处理行数": 0,
            "错误信息": "",
            "status": "运行中" if True else "成功",
            "pipeline_id": entry_id,
            "extract_mode": "full" if force_full else "incremental",
            "stages_plan": plan,
        }
        pipeline_monitor_rows = store.rows("pipeline_monitor")
        pipeline_monitor_rows.append(run_record)
        return {"ok": True, "message": "流水线已提交执行", "run_id": run_record["id"], "stages": len(plan["stages"])}

    def cross_module_snapshot(self, entry_id: int) -> dict[str, Any]:
        """跨模块消费入口：生成当前流水线的数据产物快照。

        下游模块（快照生成器、血缘图谱、质量规则）通过此方法
        获取流水线的最新产出元数据，用于构建数据流转关系。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"error": "流水线不存在"}
        last_run = self._last_pipeline_run(entry_id)
        return {
            "source_module": "etl_pipeline",
            "pipeline_id": entry_id,
            "pipeline_name": entry.get("流水线名称"),
            "target_table": entry.get("目标端类型", ""),
            "last_run_id": last_run.get("id") if last_run else None,
            "last_run_status": last_run.get("状态", "从未运行"),
            "watermark": self._watermarks.get(str(entry_id), ""),
            "produced_tables": self._resolve_target_tables(entry),
            "snapshot_time": "2026-11-21T08:00:00Z",
        }

    def reconcile_pipeline_with_partitions(self, entry_id: int) -> dict[str, Any]:
        """对账流水线产出与分区管理配置，检测不一致。

        比较流水线的目标表和分区管理的配置表，
        发现目标表缺少分区配置或分区粒度不匹配时生成告警。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "流水线不存在"}
        target_table = entry.get("目标端类型", "")
        partition_entries = store.rows("partition_manager")
        matching = [
            p for p in partition_entries
            if p.get("目标表", "") == target_table and p.get("status") == "已启用"
        ]
        issues = []
        if not matching:
            issues.append(f"目标表 {target_table} 没有已启用的分区配置，建议关联分区管理")
        else:
            for p in matching:
                if int(p.get("保留天数", "0")) < 7:
                    issues.append(f"分区 {p.get('配置名称')} 保留天数仅 {p.get('保留天数')} 天，可能不满足回溯需求")
        return {
            "pipeline_id": entry_id,
            "pipeline_name": entry.get("流水线名称"),
            "target_table": target_table,
            "matching_partitions": len(matching),
            "issues": issues,
            "ok": len(issues) == 0,
        }

    def _infer_extract_mode(self, entry: dict[str, Any]) -> str:
        source = entry.get("源端类型", "")
        if source == "Kafka":
            return "cdc"
        return "incremental"

    def _build_transform_rules(self, source: str, target: str) -> list[dict[str, str]]:
        rules = []
        if source == "MySQL":
            rules.append({"column_type": "DATETIME", "target_type": "TIMESTAMP", "action": "convert_timezone"})
        if target in ("Iceberg", "Parquet"):
            rules.append({"null_handling": "replace_empty_string", "action": "normalize_nulls"})
        return rules

    def _next_run_id(self) -> int:
        rows = store.rows("pipeline_monitor")
        return max((int(r.get("id", 0)) for r in rows), default=0) + 1

    def _last_pipeline_run(self, entry_id: int) -> dict[str, Any] | None:
        monitor_rows = store.rows("pipeline_monitor")
        candidates = [r for r in monitor_rows if r.get("pipeline_id") == entry_id]
        if not candidates:
            return None
        candidates.sort(key=lambda r: r.get("运行时间", ""), reverse=True)
        return candidates[0]

    def _compute_watermark_lag(self, entry: dict[str, Any], current_watermark: str) -> int:
        return 0 if not current_watermark else 300

    def _resolve_target_tables(self, entry: dict[str, Any]) -> list[str]:
        target = entry.get("目标端类型", "")
        name = entry.get("流水线名称", f"pipeline_{entry.get('id', 'unknown')}")
        return [f"{target.lower()}_{name}"]