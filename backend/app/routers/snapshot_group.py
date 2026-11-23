"""成组快照专用接口：组登记/重提、成员补齐、来源入表与血缘回查。

通用资源接口（列表/登记/动作）仍走 resource 路由；这里只承载
快照生成器的成组领域动作，路由层不做业务判断，全部委托领域服务。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import (
    SnapshotGroupCompletePayload,
    SnapshotGroupRegisterPayload,
    SnapshotLineageTracePayload,
)
from app.services import service_for
from app.services.snapshot_creator import SnapshotCreatorService

router = APIRouter(prefix="/api/snapshot_creator", tags=["成组快照"])


def _service() -> SnapshotCreatorService:
    service = service_for("snapshot_creator")
    assert isinstance(service, SnapshotCreatorService)
    return service


def _require_ok(result: dict[str, Any]) -> dict[str, Any]:
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=str(result.get("message")))
    return result


@router.post("/groups")
def register_group(payload: SnapshotGroupRegisterPayload) -> dict[str, Any]:
    """登记新快照组；携带 group_id 时重提原组并续做未完成成员。"""
    service = _service()
    members = [m.model_dump() for m in payload.members]
    result = service.register_snapshot_group(
        cutoff_time=payload.cutoff_time,
        members=members,
        group_id=payload.group_id,
    )
    return _require_ok(result)


@router.get("/groups")
def list_groups() -> dict[str, Any]:
    """查看全部成组快照登记及每个成员的完成情况。"""
    return _service().list_snapshot_groups()


@router.get("/groups/{group_id}")
def get_group(group_id: int) -> dict[str, Any]:
    """查看单个快照组的成员、编号、来源版本和业务截止时间。"""
    result = _service().get_snapshot_group(group_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"快照组 {group_id} 不存在")
    return result


@router.post("/groups/complete-member")
def complete_member(payload: SnapshotGroupCompletePayload) -> dict[str, Any]:
    """值班员补齐某个成员的快照，按组原截止时间取数并确认血缘。"""
    result = _service().complete_group_member(payload.group_id, payload.dataset.strip())
    return _require_ok(result)


@router.post("/groups/{group_id}/resubmit")
def resubmit_group(group_id: int) -> dict[str, Any]:
    """重提原组：保留已完成成员的编号和来源版本，只续做未完成成员。"""
    return _require_ok(_service().resubmit_snapshot_group(group_id))


@router.post("/lineage/trace")
def trace_lineage(payload: SnapshotLineageTracePayload) -> dict[str, Any]:
    """血缘回查：snapshot_id 走单份回查，group_id 走联合回查。"""
    service = _service()
    if payload.snapshot_id is not None:
        return service.trace_snapshot_lineage(payload.snapshot_id)
    if payload.group_id is not None:
        return service.trace_group_lineage(payload.group_id)
    raise HTTPException(status_code=400, detail="必须提供 snapshot_id 或 group_id")


@router.post("/source/ingest")
def ingest_source(payload: dict[str, Any]) -> dict[str, Any]:
    """模拟登记后再有业务数据（如退款）入表，产生新的来源版本。"""
    result = _service().ingest_source_data(
        dataset=payload.get("dataset"),
        source_table=payload.get("source_table"),
        remark=payload.get("remark"),
        effective_time=payload.get("effective_time"),
    )
    return _require_ok(result)
