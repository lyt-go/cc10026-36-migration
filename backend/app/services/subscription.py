"""订阅管理领域服务：管理API订阅申请、审批流转和续期。

跨模块数据流：
- 消费 api_registry 共享快照 → 校验订阅API是否存在
- 审批通过后 → usage_billing 消费用量计量
- 审批通过后 → call_audit 消费调用审计
- 订阅变更 → 写 subscriptions/ 共享记录
- 审批流转完整记录到共享目录
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SubscriptionService(BaseModuleService):
    MODULE = "subscription"
    ENTITY = "订阅申请"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["申请编号", "订阅API", "申请方", "调用频率", "数据范围", "有效期", "审批状态", "驳回原因"]
    REQUIRED_FIELDS = ["申请编号", "订阅API", "申请方"]
    STATUS_ORDER = ["草稿", "待审批", "已通过", "已驳回"]
    ACTION_RULES = {"提交审批": "待审批", "审批通过": "已通过", "驳回申请": "已驳回"}
    NEGATIVE_ACTIONS = ["驳回申请"]

    def _validate_api_exists(self, sub_data: dict[str, Any]) -> bool:
        """校验被订阅的API是否已注册。"""
        from app.services.data_manager import read_api_snapshot
        api_name = sub_data.get("订阅API", "")
        registry = read_api_snapshot()
        for api_data in (registry.values() if isinstance(registry, dict) else []):
            if isinstance(api_data, dict) and api_data.get("api_name") == api_name:
                return True
        return False

    def _write_subscription_record(self, sub_id: int, sub_data: dict[str, Any]) -> None:
        """将订阅记录写入共享目录。"""
        from app.services.data_manager import write_subscription
        record = {
            "application_no": sub_data.get("申请编号"),
            "api_name": sub_data.get("订阅API"),
            "applicant": sub_data.get("申请方"),
            "call_frequency": sub_data.get("调用频率"),
            "data_scope": sub_data.get("数据范围"),
            "valid_until": sub_data.get("有效期"),
            "approval_status": sub_data.get("status"),
            "reject_reason": sub_data.get("驳回原因", ""),
        }
        write_subscription(sub_id, record)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "提交审批":
            if not self._validate_api_exists(entry):
                return None, "订阅的目标API尚未注册，无法提交审批"
            self._write_subscription_record(entry_id, entry)

        if action == "审批通过":
            # 联动增强：创建用量初始化记录（经用量台账在途 WAL，恢复不漏记）
            from app.services import usage_ledger
            from app.store import store as s

            usage_rows = s.rows("usage_billing")
            billing_id = max((int(r.get("id", 0)) for r in usage_rows), default=0) + 1
            usage_ledger.apply_live_write({
                "usage_id": billing_id,
                "subscription_id": entry_id,
                "record_no": f"USAG-{billing_id:04d}",
                "applicant": entry.get("申请方"),
                "api_name": entry.get("订阅API"),
                "call_count": 0,
                "data_rows": 0,
                "amount": 0,
                "billing_cycle": self._current_billing_cycle(),
                "settlement_status": "计量中",
            }, increment={"call_count": 0, "data_rows": 0, "amount": 0}, is_new=True)

            # 联动增强：审批通过后自动创建限流策略
            rate_rows = s.rows("rate_limiter")
            rate_id = max((int(r.get("id", 0)) for r in rate_rows), default=0) + 1
            rate_rows.append({
                "id": rate_id,
                "策略编号": f"RATE-{rate_id:04d}",
                "目标API": entry.get("订阅API"),
                "调用方": entry.get("申请方"),
                "每秒上限": "10",
                "每分钟上限": "600",
                "每日上限": entry.get("调用频率", "10000"),
                "超限动作": "延迟重试",
                "策略版本": "v1",
                "status": "已生效",
                "pending": False,
                "abnormal": False,
                "_auto_created": True,
            })

            # 联动增强：自动记录审计——审批通过事件
            from app.services.data_manager import write_audit, DATA_ROOT
            audit_id = max((int(f.stem) for f in DATA_ROOT.glob("audit/[0-9]*.json")), default=0) + 1
            write_audit(audit_id, {
                "audit_id": audit_id,
                "api_name": entry.get("订阅API"),
                "caller": entry.get("申请方"),
                "event": "subscription_approved",
                "subscription_id": entry_id,
                "approval_status": "已通过",
            })

        result = super().run_action(entry_id, action)

        # 更新共享订阅记录
        if entry:
            self._write_subscription_record(entry_id, entry)

        # 写模块快照
        from app.services.data_manager import write_module_snapshot
        write_module_snapshot(self.MODULE, self.domain_snapshot())

        return result

    def _current_billing_cycle(self) -> str:
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone(timedelta(hours=8)))
        return now.strftime("%Y-%m")

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        sub_list = []
        for row in rows:
            sub_list.append({
                "id": row.get("id"),
                "app_no": row.get("申请编号"),
                "api_name": row.get("订阅API"),
                "applicant": row.get("申请方"),
                "frequency": row.get("调用频率"),
                "scope": row.get("数据范围"),
                "valid_until": row.get("有效期"),
                "status": row.get("status"),
            })
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(sub_list),
            "subscriptions": sub_list,
            "approved_count": sum(1 for r in sub_list if r.get("status") == "已通过"),
            "pending_count": sum(1 for r in sub_list if r.get("status") == "待审批"),
        }