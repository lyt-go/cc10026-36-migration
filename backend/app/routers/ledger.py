"""用量台账迁移流水线接口：迁移推进、进度、确认视图、双方对照、结算与恢复。

只负责编排，状态流转全部在 app.services.usage_ledger 里完成。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from app.services import usage_ledger

router = APIRouter(prefix="/api/ledger", tags=["用量台账"])


@router.post("/migrate/{billing_cycle}")
def migrate(billing_cycle: str) -> dict[str, Any]:
    """推进（或断点恢复）一个账期的迁移流水线。"""
    return usage_ledger.migrate_cycle(billing_cycle)


@router.get("/migration")
def migration_status_all() -> dict[str, Any]:
    """全部账期的迁移进度。"""
    return usage_ledger.migration_status()


@router.get("/migration/{billing_cycle}")
def migration_status_one(billing_cycle: str) -> dict[str, Any]:
    return usage_ledger.migration_status(billing_cycle)


@router.get("/query/{billing_cycle}")
def query_cycle(billing_cycle: str) -> dict[str, Any]:
    """同一账期的确认视图：并存期间只含双方相符的确认结果。"""
    return usage_ledger.query_cycle(billing_cycle)


@router.get("/reconcile/{billing_cycle}")
def reconcile_cycle(billing_cycle: str) -> dict[str, Any]:
    """双方对照：旧目录视图与台账视图并排，列出逐单差异。"""
    return usage_ledger.reconcile_cycle(billing_cycle)


@router.post("/settle/{billing_cycle}")
def settle_cycle(billing_cycle: str) -> dict[str, Any]:
    """生成结算：基于确认视图，幂等；冲突时拒绝并附双方视图。"""
    return usage_ledger.settle_cycle(billing_cycle)


@router.post("/recover")
def recover() -> dict[str, Any]:
    """恢复流程：重放在途请求，保证不漏记不重记。"""
    return usage_ledger.recover_inflight()


@router.get("/inflight")
def inflight() -> dict[str, Any]:
    return usage_ledger.inflight_status()
