"""用量计量领域服务：管理API调用用量计量和计费周期结算。

跨模块数据流：
- 消费 api_registry 共享快照 → 核验计费API
- 消费 subscription 共享快照 → 审批通过后初始化用量
- 消费 rate_limiter 快照 → 超限写入用量异常
- 写入 data/usage/ 共享文件 → call_audit 消费审计
- 计费周期结束后触发结算 → 写入结算汇总
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class UsageBillingService(BaseModuleService):
    MODULE = "usage_billing"
    ENTITY = "用量记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "调用方", "API名称", "调用次数", "数据行数", "计费周期", "费用金额", "结算状态"]
    REQUIRED_FIELDS = ["记录编号", "调用方", "API名称"]
    STATUS_ORDER = ["计量中", "已出账", "已确认", "已结算"]
    ACTION_RULES = {"确认出账": "已出账", "标记确认": "已确认", "完成结算": "已结算"}
    NEGATIVE_ACTIONS = []

    def record_usage(self, api_name: str, caller: str, call_count: int = 1,
                     data_rows: int = 0, amount: float = 0.0) -> dict[str, Any]:
        """记录一次API调用用量。持久化经用量台账在途 WAL，恢复不漏记。"""
        from app.services import usage_ledger
        from app.store import store as s

        rows = s.rows(self.MODULE)
        # 查找已有记录
        billing_cycle = self._current_billing_cycle()
        for row in rows:
            if (row.get("API名称") == api_name and row.get("调用方") == caller
                    and row.get("计费周期") == billing_cycle):
                old_count = int(row.get("调用次数", "0") or 0)
                old_rows = int(row.get("数据行数", "0") or 0)
                old_amount = float(row.get("费用金额", 0) or 0)
                row["调用次数"] = str(old_count + call_count)
                row["数据行数"] = str(old_rows + data_rows)
                row["费用金额"] = old_amount + amount
                usage_ledger.apply_live_write({
                    "usage_id": row.get("id", 0),
                    "record_no": row.get("记录编号"),
                    "caller": caller,
                    "api_name": api_name,
                    "call_count": old_count + call_count,
                    "data_rows": old_rows + data_rows,
                    "amount": old_amount + amount,
                    "billing_cycle": billing_cycle,
                    "settlement_status": row.get("status"),
                }, increment={"call_count": call_count, "data_rows": data_rows,
                              "amount": amount}, is_new=False)
                return row

        # 新记录
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        new_record = {
            "id": new_id,
            "记录编号": f"USAG-{new_id:04d}",
            "调用方": caller,
            "API名称": api_name,
            "调用次数": str(call_count),
            "数据行数": str(data_rows),
            "计费周期": billing_cycle,
            "费用金额": amount,
            "结算状态": self.STATUS_ORDER[0],
            "status": self.STATUS_ORDER[0],
            "pending": True,
            "abnormal": False,
        }
        rows.append(new_record)
        usage_ledger.apply_live_write({
            "usage_id": new_id,
            "record_no": f"USAG-{new_id:04d}",
            "caller": caller,
            "api_name": api_name,
            "call_count": call_count,
            "data_rows": data_rows,
            "amount": amount,
            "billing_cycle": billing_cycle,
            "settlement_status": self.STATUS_ORDER[0],
        }, increment={"call_count": call_count, "data_rows": data_rows,
                      "amount": amount}, is_new=True)
        return new_record

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        result = super().run_action(entry_id, action)

        if action == "完成结算" and entry:
            from app.services import usage_ledger
            billing_cycle = entry.get("计费周期", self._current_billing_cycle())
            # 结算与查询共用用量台账确认视图；并存期间冲突会被拒绝并可对照
            settlement = usage_ledger.settle_cycle(billing_cycle, auto_settlement=True)

            # 联动增强：结算完成写入调用审计记录
            from app.services.data_manager import write_audit, DATA_ROOT
            audit_id = max((int(f.stem) for f in DATA_ROOT.glob("audit/[0-9]*.json")), default=0) + 1
            write_audit(audit_id, {
                "audit_id": audit_id,
                "api_name": entry.get("API名称", ""),
                "caller": entry.get("调用方", ""),
                "event": "billing_settled",
                "billing_cycle": billing_cycle,
                "total_amount": settlement.get("total_amount", 0),
                "total_calls": settlement.get("total_calls", 0),
            })

        from app.services.data_manager import write_module_snapshot
        write_module_snapshot(self.MODULE, self.domain_snapshot())
        return result

    def _current_billing_cycle(self) -> str:
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m")

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        usage_list = []
        for row in rows:
            usage_list.append({
                "id": row.get("id"),
                "record_no": row.get("记录编号"),
                "caller": row.get("调用方"),
                "api_name": row.get("API名称"),
                "call_count": row.get("调用次数"),
                "data_rows": row.get("数据行数"),
                "billing_cycle": row.get("计费周期"),
                "amount": row.get("费用金额"),
                "status": row.get("status"),
            })
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(usage_list),
            "usages": usage_list,
            "total_amount": sum(float(r.get("amount", 0) or 0) for r in usage_list),
            "settled_count": sum(1 for r in usage_list if r.get("status") == "已结算"),
        }