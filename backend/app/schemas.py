"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class SnapshotGroupMemberPayload(BaseModel):
    """成组快照中的单个成员数据集。"""

    dataset: str
    snapshot_type: str = "full"


class SnapshotGroupRegisterPayload(BaseModel):
    """成组快照登记/重提载荷。

    不传 group_id 表示新建一组；传入已存在的 group_id 表示重提原组，
    此时只续做尚未完成的成员，已完成成员的编号和来源版本保持不变。
    """

    cutoff_time: str
    members: list[SnapshotGroupMemberPayload]
    group_id: int | None = None
    remark: str | None = None


class SnapshotGroupCompletePayload(BaseModel):
    """补齐组内某个成员的快照（值班员补做）。"""

    group_id: int
    dataset: str
    remark: str | None = None


class SnapshotLineageTracePayload(BaseModel):
    """成组快照血缘回查参数：单份回查传 snapshot_id，联合回查传 group_id。"""

    snapshot_id: int | None = None
    group_id: int | None = None
