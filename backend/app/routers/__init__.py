"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import ledger, resource

# 台账流水线路由含固定前缀 /api/ledger，必须先于通用 /api/{module} 注册
ROUTERS = [ledger, resource]
