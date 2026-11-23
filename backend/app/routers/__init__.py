"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, snapshot_group

# 成组快照的专用路径需在通用 /api/{module} 动态路由之前注册，
# 避免固定路径被资源路由抢先匹配。
ROUTERS = [snapshot_group, resource]
