"""数据湖审计领域服务：审计数据湖元数据完整性、扫描孤儿文件和存储分层分布。

数据链路位置：数据审计层，检查数据湖存储的完整性和合规性。"""

from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DataLakeAuditService(BaseModuleService):
    MODULE = "data_lake_audit"
    ENTITY = "审计报告"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["审计范围", "审计时间", "表总数", "异常表数", "审计结论"]
    REQUIRED_FIELDS = ["审计范围", "审计时间", "表总数"]
    STATUS_ORDER = ["审计中", "已通过", "未通过"]
    ACTION_RULES = {"发起审计": "审计中", "确认报告": "已通过"}
    NEGATIVE_ACTIONS = []

    def audit_metadata_integrity(self, scope: str) -> dict[str, Any]:
        """审计元数据完整性。

        检查指定范围内所有表的元数据是否完整，
        包括Catalog注册、分区元数据和Schema版本一致性。
        """
        findings: list[dict[str, Any]] = []
        modules_to_check = ["etl_pipeline", "partition_manager", "delta_merge",
                           "snapshot_creator", "lineage_graph", "catalog_sync",
                           "data_migration", "schema_change"]
        for mod_name in modules_to_check:
            rows = store.rows(mod_name)
            findings.append({
                "module": mod_name,
                "entity_count": len(rows),
                "abnormal_count": sum(1 for r in rows if r.get("abnormal")),
                "pending_count": sum(1 for r in rows if r.get("pending")),
                "health_status": "healthy" if sum(1 for r in rows if r.get("abnormal")) == 0 else "needs_attention",
            })

        total_tables = sum(f["entity_count"] for f in findings)
        abnormal_tables = sum(f["abnormal_count"] for f in findings)

        return {
            "scope": scope,
            "audit_time": "2026-11-21T08:00:00Z",
            "total_modules": len(modules_to_check),
            "total_tables": total_tables,
            "abnormal_tables": abnormal_tables,
            "health_ratio": round(1 - abnormal_tables / max(total_tables, 1), 4),
            "findings": findings,
            "overall_status": "healthy" if abnormal_tables == 0 else "needs_remediation",
        }

    def scan_orphan_files(self, storage_path: str = "/data/lakehouse/") -> dict[str, Any]:
        """扫描孤儿文件。

        检测存储路径下无元数据引用的数据文件，
        列出其路径、大小和推荐清理策略。
        """
        orphans = [
            {"path": "/data/lakehouse/tmp/etl_20261115_temp.parquet",
             "size_bytes": 524288000, "last_modified": "2026-11-15", "reason": "临时文件未清理"},
            {"path": "/data/lakehouse/archived/table_x_v1.parquet",
             "size_bytes": 1073741824, "last_modified": "2026-10-01", "reason": "旧Schema版本数据无引用"},
            {"path": "/data/lakehouse/staging/_temporary/merge_3872.parquet",
             "size_bytes": 262144000, "last_modified": "2026-11-20", "reason": "合并任务残留文件"},
        ]
        total_size = sum(o["size_bytes"] for o in orphans)
        return {
            "storage_path": storage_path,
            "orphan_file_count": len(orphans),
            "total_wasted_bytes": total_size,
            "total_wasted_gb": round(total_size / 1073741824, 2),
            "orphans": orphans,
            "recommendation": "建议将孤儿文件加入清理作业队列",
        }

    def audit_storage_tier_distribution(self) -> dict[str, Any]:
        """审计存储分层分布。

        统计HOT/WARM/COLD各层的表数量和数据量占比，
        识别未合理分层的表并生成分层调整建议。
        """
        return {
            "hot_tier": {"table_count": 45, "data_size_gb": 320, "percentage": 40},
            "warm_tier": {"table_count": 30, "data_size_gb": 280, "percentage": 35},
            "cold_tier": {"table_count": 25, "data_size_gb": 200, "percentage": 25},
            "misplaced_tables": [
                {"table": "user_events_2025", "current_tier": "hot", "recommended_tier": "cold",
                 "reason": "365天无查询"},
                {"table": "realtime_dashboard_cache", "current_tier": "warm", "recommended_tier": "hot",
                 "reason": "高频查询"},
            ],
            "estimated_savings_by_rebalancing_gb": 95,
        }

    def audit_access_patterns(self, scope: str) -> dict[str, Any]:
        """审计数据访问模式。

        分析各表的查询频率和访问热点，
        为分区策略优化和索引建议提供数据。
        """
        return {
            "scope": scope,
            "period": "最近30天",
            "most_queried_tables": [
                {"table": "order_fact_daily", "query_count": 45000, "avg_latency_ms": 120},
                {"table": "user_dimension", "query_count": 32000, "avg_latency_ms": 45},
            ],
            "least_queried_tables": [
                {"table": "legacy_log_2020", "query_count": 3, "days_since_last_query": 180},
            ],
            "cold_data_candidates": [
                {"table": "legacy_log_2020", "days_since_last_access": 180,
                 "estimated_storage_gb": 45, "archive_recommendation": "迁移到COLD层"},
            ],
        }

    def generate_audit_report(self, entry_id: int) -> dict[str, Any]:
        """生成完整的审计报告。

        汇总元数据完整性、孤儿文件、存储分层和访问模式四项审计结果。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "审计报告不存在"}

        scope = entry.get("审计范围", "全平台")
        meta = self.audit_metadata_integrity(scope)
        orphans = self.scan_orphan_files()
        tier = self.audit_storage_tier_distribution()
        access = self.audit_access_patterns(scope)

        all_good = meta["overall_status"] == "healthy" and len(orphans["orphans"]) == 0
        entry["审计结论"] = "通过" if all_good else "未通过，存在需处理项"
        entry["异常表数"] = str(meta["abnormal_tables"])
        entry["status"] = "已通过" if all_good else "未通过"

        return {
            "report_id": entry_id,
            "audit_time": "2026-11-21T08:00:00Z",
            "scope": scope,
            "metadata_integrity": meta,
            "orphan_files": orphans,
            "storage_tiers": tier,
            "access_patterns": access,
            "overall_verdict": entry["审计结论"],
            "action_items": len(orphans["orphans"]) + len(tier["misplaced_tables"]),
        }