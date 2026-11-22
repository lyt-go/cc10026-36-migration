"""用量台账迁移与恢复测试。

场景：数据库迁移流水线上线时，同一账期有两类历史明细（已结/未结）。
迁入已结和未结账期各一份，分别在写新用量、生成结算和切换读取阶段
中止一次，再核对恢复结果与尝试前的记录。

覆盖：
1. 已结账期（含结算汇总）迁入：原明细身份、结算状态、汇总引用保留；
2. 未结账期迁入：逐单对照一致后切换读取；
3. 写新用量阶段中止：恢复重放在途请求，不漏记、不重记；
4. 生成结算阶段中止：恢复后汇总与尝试前记录核对一致，幂等；
5. 切换读取阶段中止：恢复后切换完成，查询与尝试前记录一致；
6. 迁移与当前记账冲突：双方可对照，结算拒绝放行；
7. 查询与结算共用同一台账数据，可查应收金额与新写入层次。

pytest 风格；环境无 pytest 时可直接 `python3 test_usage_ledger_migration.py` 运行。
"""

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# 先占位，setup_method 里每个用例会换成独立临时目录并同步 data_manager.DATA_ROOT
os.environ.setdefault("DATA_ROOT", tempfile.mkdtemp(prefix="ledger_test_boot_"))

from app.services import data_manager, usage_ledger  # noqa: E402


# 已结账期：含结算汇总
CYCLE_SETTLED = "2026-10"
# 未结账期：无结算汇总
CYCLE_OPEN = "2026-11"
# 切换读取中止专用账期（已结/未结各一）
CYCLE_SETTLED_SWITCH = "2026-09"
CYCLE_OPEN_SWITCH = "2026-08"


class TestUsageLedgerMigration:
    """用量台账迁移流水线与崩溃恢复"""

    def setup_method(self):
        self.tmp = tempfile.mkdtemp(prefix="ledger_test_")
        os.environ["DATA_ROOT"] = self.tmp
        data_manager.DATA_ROOT = Path(self.tmp)
        self._build_fixtures()

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ----------------------------------------------------------
    # 历史数据夹具：按记录分散保存的用量 JSON + 结算汇总
    # ----------------------------------------------------------

    def _build_fixtures(self):
        # 已结账期 2026-10：三条已结算明细 + 结算汇总
        self._write_usage(101, record_no="USAG-0101", caller="甲方", api_name="订单API",
                          call_count=100, data_rows=5000, amount=120.5,
                          billing_cycle=CYCLE_SETTLED, settlement_status="已结算")
        self._write_usage(102, record_no="USAG-0102", caller="乙方", api_name="订单API",
                          call_count=50, data_rows=2000, amount=60.0,
                          billing_cycle=CYCLE_SETTLED, settlement_status="已结算")
        self._write_usage(103, record_no="USAG-0103", caller="甲方", api_name="库存API",
                          call_count=20, data_rows=800, amount=30.0,
                          billing_cycle=CYCLE_SETTLED, settlement_status="已结算")
        self._write_summary(CYCLE_SETTLED, total_amount=210.5, total_calls=170, record_count=3)

        # 未结账期 2026-11：两条未结明细，无结算汇总
        self._write_usage(201, record_no="USAG-0201", caller="丙方", api_name="订单API",
                          call_count=10, data_rows=300, amount=15.0,
                          billing_cycle=CYCLE_OPEN, settlement_status="计量中")
        self._write_usage(202, record_no="USAG-0202", caller="丁方", api_name="物流API",
                          call_count=7, data_rows=210, amount=8.75,
                          billing_cycle=CYCLE_OPEN, settlement_status="已出账")

        # 切换读取中止专用：已结账期 2026-09（单条 + 汇总）
        self._write_usage(301, record_no="USAG-0301", caller="甲方", api_name="报表API",
                          call_count=40, data_rows=1200, amount=88.0,
                          billing_cycle=CYCLE_SETTLED_SWITCH, settlement_status="已结算")
        self._write_summary(CYCLE_SETTLED_SWITCH, total_amount=88.0, total_calls=40, record_count=1)

        # 切换读取中止专用：未结账期 2026-08（单条，无汇总）
        self._write_usage(401, record_no="USAG-0401", caller="乙方", api_name="报表API",
                          call_count=5, data_rows=150, amount=6.25,
                          billing_cycle=CYCLE_OPEN_SWITCH, settlement_status="计量中")

    def _write_usage(self, usage_id, **fields):
        data_manager.write_usage(usage_id, {"usage_id": usage_id, **fields})

    def _write_summary(self, cycle, **fields):
        summary = {"billing_cycle": cycle, "settled_at": "2026-10-31T23:59:59+08:00",
                   "auto_settlement": True, **fields}
        data_manager._write_json(
            Path(self.tmp) / "usage" / f"_settlement_{cycle}.json", summary)

    def _json_snapshot(self, cycle):
        """尝试前的记录：旧目录中该账期明细的逐单快照。"""
        return {r["usage_id"]: r for r in usage_ledger._json_cycle_records(cycle)}

    # ==========================================================
    # 1. 迁入已结账期：原单身份、结算状态与汇总引用保留
    # ==========================================================

    def test_migrate_settled_cycle_preserves_identity_and_summary(self):
        result = usage_ledger.migrate_cycle(CYCLE_SETTLED)
        assert result["phase"] == "done", f"迁移应完成: {result}"

        # 原明细身份与结算状态保留
        view = usage_ledger._db_cycle_view(CYCLE_SETTLED)
        by_id = {r["usage_id"]: r for r in view}
        assert set(by_id) == {101, 102, 103}, "原单 usage_id 必须保留"
        assert by_id[101]["record_no"] == "USAG-0101"
        assert by_id[101]["caller"] == "甲方"
        assert by_id[102]["amount"] == 60.0
        assert by_id[103]["call_count"] == 20
        for rec in view:
            assert rec["settlement_status"] == "已结算", "结算状态必须保留"
            assert rec["settlement_ref"] == CYCLE_SETTLED, "汇总引用必须关联到账期"
            base_layers = [l for l in rec["layers"] if l["layer"] == "base"]
            assert base_layers and base_layers[0]["source"] == "json_migration"

        # 结算汇总与汇总引用（settlement_item）保留
        summary = usage_ledger._db_summary(CYCLE_SETTLED)
        assert summary is not None, "已结账期的结算汇总必须迁入"
        assert summary["total_amount"] == 210.5
        assert summary["total_calls"] == 170
        assert summary["record_count"] == 3
        assert summary["source"] == "json_migration"
        items = usage_ledger._db_summary_items(CYCLE_SETTLED)
        assert {i["usage_id"] for i in items} == {101, 102, 103}
        assert sum(i["amount"] for i in items) == 210.5

        # 切换后查询走台账，且与旧目录对得上
        query = usage_ledger.query_cycle(CYCLE_SETTLED)
        assert query["source"] == "db"
        assert query["total_amount"] == 210.5
        assert query["total_calls"] == 170
        reconcile = usage_ledger.reconcile_cycle(CYCLE_SETTLED)
        assert reconcile["consistent"], f"迁移后双方必须相符: {reconcile['conflicts']}"

    # ==========================================================
    # 2. 迁入未结账期
    # ==========================================================

    def test_migrate_unsettled_cycle(self):
        result = usage_ledger.migrate_cycle(CYCLE_OPEN)
        assert result["phase"] == "done", f"迁移应完成: {result}"

        view = usage_ledger._db_cycle_view(CYCLE_OPEN)
        by_id = {r["usage_id"]: r for r in view}
        assert set(by_id) == {201, 202}
        assert by_id[201]["settlement_status"] == "计量中", "未结状态必须保留"
        assert by_id[202]["settlement_status"] == "已出账"
        assert by_id[201]["settlement_ref"] is None, "未结明细不应有汇总引用"
        assert usage_ledger._db_summary(CYCLE_OPEN) is None, "未结账期不应生成汇总"

        query = usage_ledger.query_cycle(CYCLE_OPEN)
        assert query["source"] == "db"
        assert abs(query["total_amount"] - 23.75) < 1e-9
        assert usage_ledger.reconcile_cycle(CYCLE_OPEN)["consistent"]

    # ==========================================================
    # 3. 装载阶段中止：断点恢复后不少不多
    # ==========================================================

    def test_crash_mid_load_recovers_without_loss(self):
        try:
            usage_ledger.migrate_cycle(CYCLE_SETTLED, crash_at="load")
            raise AssertionError("应抛出 CrashAbort")
        except usage_ledger.CrashAbort:
            pass
        state = usage_ledger.migration_status(CYCLE_SETTLED)
        assert state["phase"] == "loading", f"中止后应停在装载阶段: {state['phase']}"
        partial = usage_ledger._db_cycle_view(CYCLE_SETTLED)
        assert len(partial) == 1, "装载中止时已提交部分明细"

        # 恢复：从断点继续，最终与尝试前记录一致
        result = usage_ledger.migrate_cycle(CYCLE_SETTLED)
        assert result["phase"] == "done"
        view = usage_ledger._db_cycle_view(CYCLE_SETTLED)
        assert len(view) == 3, "恢复后明细必须完整"
        assert len({r["usage_id"] for r in view}) == 3, "恢复不得重复入账"
        assert usage_ledger.reconcile_cycle(CYCLE_SETTLED)["consistent"]

    # ==========================================================
    # 4. 写新用量阶段中止：已结/未结账期各中止一次，在途请求不漏不重
    # ==========================================================

    def test_crash_writing_new_usage_recovers_inflight(self):
        for cycle, existing_id, new_id in (
                (CYCLE_SETTLED, 101, 109), (CYCLE_OPEN, 201, 209)):
            usage_ledger.migrate_cycle(cycle)
            pre = usage_ledger.query_cycle(cycle)
            pre_total = pre["total_amount"]
            pre_by_id = {r["usage_id"]: r for r in pre["records"]}

            # 4a. 新记录在「记 WAL 后、应用前」中止 → 恢复必须补上（不漏记）
            try:
                usage_ledger.apply_live_write(
                    {"usage_id": new_id, "record_no": f"USAG-{new_id:04d}",
                     "caller": "在途方", "api_name": "订单API",
                     "call_count": 4, "data_rows": 40, "amount": 9.5,
                     "billing_cycle": cycle, "settlement_status": "计量中"},
                    increment={"call_count": 4, "data_rows": 40, "amount": 9.5},
                    is_new=True, crash_at="write_journal")
                raise AssertionError("应抛出 CrashAbort")
            except usage_ledger.CrashAbort:
                pass
            assert usage_ledger.inflight_status().get("pending") == 1, "在途请求必须留痕"
            mid = usage_ledger.query_cycle(cycle)
            assert mid["total_amount"] == pre_total, "应用前中止不得产生半笔写入"

            recovery = usage_ledger.recover_inflight()
            assert recovery["replayed"] == 1, "恢复必须重放在途请求"
            post = usage_ledger.query_cycle(cycle)
            assert abs(post["total_amount"] - (pre_total + 9.5)) < 1e-9
            new_rec = {r["usage_id"]: r for r in post["records"]}[new_id]
            assert new_rec["amount"] == 9.5, "在途新记录必须补上且只补一次"

            # 4b. 既有记录在「应用后、标记前」中止 → 恢复重放不得重记
            base_amount = pre_by_id[existing_id]["amount"]
            base_calls = pre_by_id[existing_id]["call_count"]
            base_rows = pre_by_id[existing_id]["data_rows"]
            cur_status = pre_by_id[existing_id]["settlement_status"]
            try:
                usage_ledger.apply_live_write(
                    {"usage_id": existing_id, "record_no": f"USAG-{existing_id:04d}",
                     "caller": pre_by_id[existing_id]["caller"], "api_name": "订单API",
                     "call_count": base_calls + 4, "data_rows": base_rows + 40,
                     "amount": base_amount + 2.0,
                     "billing_cycle": cycle, "settlement_status": cur_status},
                    increment={"call_count": 4, "data_rows": 40, "amount": 2.0},
                    is_new=False, crash_at="write_apply")
                raise AssertionError("应抛出 CrashAbort")
            except usage_ledger.CrashAbort:
                pass
            recovery = usage_ledger.recover_inflight()
            assert recovery["replayed"] == 1
            final = usage_ledger.query_cycle(cycle)
            final_rec = {r["usage_id"]: r for r in final["records"]}[existing_id]
            assert abs(final_rec["amount"] - (base_amount + 2.0)) < 1e-9, \
                "重放不得重复入账（不重记）"
            # 层次：原记录层不动，新写入层只记一次增量
            assert abs(final_rec["base_amount"] - base_amount) < 1e-9
            assert abs(final_rec["delta_amount"] - 2.0) < 1e-9
            deltas = [l for l in final_rec["layers"] if l["layer"] == "delta"]
            assert len(deltas) == 1, "同一在途请求只能入账一次"

            # 双方仍相符；恢复幂等
            assert usage_ledger.reconcile_cycle(cycle)["consistent"]
            assert usage_ledger.recover_inflight()["replayed"] == 0

    # ==========================================================
    # 5. 生成结算阶段中止：已结/未结账期各中止一次，恢复后与尝试前核对
    # ==========================================================

    def test_crash_generating_settlement_recovers_consistent(self):
        # 已结账期：DB 事务提交后、旧目录镜像前中止
        usage_ledger.migrate_cycle(CYCLE_SETTLED)
        pre_records = usage_ledger.query_cycle(CYCLE_SETTLED)["records"]
        pre_summary = usage_ledger._db_summary(CYCLE_SETTLED)
        pre_items = usage_ledger._db_summary_items(CYCLE_SETTLED)
        try:
            usage_ledger.settle_cycle(CYCLE_SETTLED, crash_at="settle_db")
            raise AssertionError("应抛出 CrashAbort")
        except usage_ledger.CrashAbort:
            pass

        recovered = usage_ledger.settle_cycle(CYCLE_SETTLED)
        assert recovered["ok"], f"恢复结算应成功: {recovered}"
        summary = usage_ledger._db_summary(CYCLE_SETTLED)
        # 与尝试前的记录核对：汇总值、结算时点、汇总引用逐项一致
        assert summary["total_amount"] == pre_summary["total_amount"] == 210.5
        assert summary["total_calls"] == pre_summary["total_calls"] == 170
        assert summary["settled_at"] == pre_summary["settled_at"], "原结算时点不得改写"
        assert usage_ledger._db_summary_items(CYCLE_SETTLED) == pre_items
        post_records = {r["usage_id"]: r for r in
                        usage_ledger.query_cycle(CYCLE_SETTLED)["records"]}
        for rec in pre_records:
            assert post_records[rec["usage_id"]]["amount"] == rec["amount"]
            assert post_records[rec["usage_id"]]["settlement_status"] == "已结算"

        # 未结账期：旧目录镜像中途（第二张单）中止
        usage_ledger.migrate_cycle(CYCLE_OPEN)
        pre_open = usage_ledger.query_cycle(CYCLE_OPEN)["records"]
        pre_open_total = round(sum(r["amount"] for r in pre_open), 6)
        pre_open_calls = sum(r["call_count"] for r in pre_open)
        try:
            usage_ledger.settle_cycle(CYCLE_OPEN, crash_at="settle_json")
            raise AssertionError("应抛出 CrashAbort")
        except usage_ledger.CrashAbort:
            pass
        # 中止现场：台账已结算，旧目录只镜像了一半
        half = {r["usage_id"]: r["settlement_status"]
                for r in usage_ledger._json_cycle_records(CYCLE_OPEN)}
        assert half[201] == "已结算" and half[202] == "已出账", "应停在镜像中途"

        recovered = usage_ledger.settle_cycle(CYCLE_OPEN)
        assert recovered["ok"], f"恢复结算应成功: {recovered}"
        summary = usage_ledger._db_summary(CYCLE_OPEN)
        # 与尝试前的记录核对：计入金额等于尝试前明细合计
        assert abs(summary["total_amount"] - pre_open_total) < 1e-9
        assert summary["total_calls"] == pre_open_calls
        assert summary["record_count"] == len(pre_open)
        items = usage_ledger._db_summary_items(CYCLE_OPEN)
        assert {i["usage_id"] for i in items} == {201, 202}
        for rec in usage_ledger._json_cycle_records(CYCLE_OPEN):
            assert rec["settlement_status"] == "已结算", "恢复后旧目录必须镜像一致"
        assert usage_ledger.reconcile_cycle(CYCLE_OPEN)["consistent"]

        # 结算幂等：再次结算不改变结果
        again = usage_ledger.settle_cycle(CYCLE_OPEN)
        assert again["ok"] and again["settled_at"] == recovered["settled_at"]
        assert usage_ledger._db_summary(CYCLE_OPEN)["total_amount"] == summary["total_amount"]

    # ==========================================================
    # 6. 切换读取阶段中止：已结/未结账期各中止一次，恢复后与尝试前核对
    # ==========================================================

    def test_crash_switching_reads_recovers_consistent(self):
        for cycle in (CYCLE_SETTLED_SWITCH, CYCLE_OPEN_SWITCH):
            pre_json = self._json_snapshot(cycle)
            try:
                usage_ledger.migrate_cycle(cycle, crash_at="switch")
                raise AssertionError("应抛出 CrashAbort")
            except usage_ledger.CrashAbort:
                pass
            state = usage_ledger.migration_status(cycle)
            assert state["phase"] == "switching", f"中止后应停在切换阶段: {state['phase']}"

            # 中止现场：尚未切换，并存期查询只见双方相符的确认结果
            mid = usage_ledger.query_cycle(cycle)
            assert mid["source"] == "dual"
            assert {r["usage_id"] for r in mid["records"]} == set(pre_json)
            assert not mid["conflicts"]

            # 恢复：完成切换，查询结果与尝试前的记录逐单一致
            result = usage_ledger.migrate_cycle(cycle)
            assert result["phase"] == "done"
            post = usage_ledger.query_cycle(cycle)
            assert post["source"] == "db"
            post_by_id = {r["usage_id"]: r for r in post["records"]}
            assert set(post_by_id) == set(pre_json), "原单身份必须保留"
            for usage_id, pre_rec in pre_json.items():
                got = post_by_id[usage_id]
                assert got["record_no"] == pre_rec["record_no"]
                assert got["amount"] == pre_rec["amount"]
                assert got["call_count"] == pre_rec["call_count"]
                assert got["settlement_status"] == pre_rec["settlement_status"]
            assert post["total_amount"] == round(
                sum(r["amount"] for r in pre_json.values()), 6)
            assert usage_ledger.reconcile_cycle(cycle)["consistent"]

    # ==========================================================
    # 7. 迁移与当前记账冲突：双方对照，结算拒绝放行
    # ==========================================================

    def test_conflict_during_migration_visible_and_settle_blocked(self):
        # 迁移停在装载中途，此时当前记账仍在写入
        try:
            usage_ledger.migrate_cycle(CYCLE_OPEN, crash_at="load")
            raise AssertionError("应抛出 CrashAbort")
        except usage_ledger.CrashAbort:
            pass
        usage_ledger.apply_live_write(
            {"usage_id": 207, "record_no": "USAG-0207", "caller": "戊方",
             "api_name": "订单API", "call_count": 3, "data_rows": 90, "amount": 4.5,
             "billing_cycle": CYCLE_OPEN, "settlement_status": "计量中"},
            increment={"call_count": 3, "data_rows": 90, "amount": 4.5}, is_new=True)

        # 双方对照：同一账期的查询结果与计入金额并排可见
        reconcile = usage_ledger.reconcile_cycle(CYCLE_OPEN)
        assert not reconcile["consistent"]
        conflict_ids = {c["usage_id"] for c in reconcile["conflicts"]}
        assert 202 in conflict_ids and 207 in conflict_ids, "未装载/在途新单都要列入冲突"
        assert reconcile["json_view"]["record_count"] == 3
        assert reconcile["db_view"]["record_count"] == 1
        assert reconcile["amount_diff"] > 0

        # 并存期查询只见双方相符的确认结果
        query = usage_ledger.query_cycle(CYCLE_OPEN)
        assert query["source"] == "dual"
        assert {r["usage_id"] for r in query["records"]} == {201}

        # 冲突时结算拒绝放行，并附双方视图
        settle = usage_ledger.settle_cycle(CYCLE_OPEN)
        assert not settle["ok"]
        assert "未对齐" in settle["error"]
        assert settle["views"]["json_view"]["total_amount"] > 0

        # 恢复迁移：装载重扫旧目录吸收在途写入，双方重新相符后结算放行
        result = usage_ledger.migrate_cycle(CYCLE_OPEN)
        assert result["phase"] == "done"
        reconcile = usage_ledger.reconcile_cycle(CYCLE_OPEN)
        assert reconcile["consistent"], f"恢复后必须相符: {reconcile['conflicts']}"
        assert reconcile["db_view"]["record_count"] == 3
        settle = usage_ledger.settle_cycle(CYCLE_OPEN)
        assert settle["ok"], f"相符后结算应放行: {settle}"
        assert abs(settle["total_amount"] - (23.75 + 4.5)) < 1e-9

    # ==========================================================
    # 8. 查询与结算共用同一台账数据；应收金额与新写入层次可查
    # ==========================================================

    def test_query_and_settle_share_same_ledger_view(self):
        usage_ledger.migrate_cycle(CYCLE_OPEN)
        # 切换后新写入：既有记录增量（新写入层）
        usage_ledger.apply_live_write(
            {"usage_id": 201, "record_no": "USAG-0201", "caller": "丙方",
             "api_name": "订单API", "call_count": 15, "data_rows": 450, "amount": 22.5,
             "billing_cycle": CYCLE_OPEN, "settlement_status": "计量中"},
            increment={"call_count": 5, "data_rows": 150, "amount": 7.5}, is_new=False)

        query = usage_ledger.query_cycle(CYCLE_OPEN)
        rec201 = {r["usage_id"]: r for r in query["records"]}[201]
        # 按原记录查到应收金额 = 原记录层 + 新写入层
        assert rec201["base_amount"] == 15.0
        assert rec201["delta_amount"] == 7.5
        assert rec201["amount"] == 22.5
        assert {l["layer"] for l in rec201["layers"]} == {"base", "delta"}

        # 结算与查询同一份台账数据：汇总等于查询合计，引用等于逐单应收
        settle = usage_ledger.settle_cycle(CYCLE_OPEN)
        assert settle["ok"]
        assert settle["total_amount"] == query["total_amount"] == 31.25
        items = {i["usage_id"]: i for i in settle["items"]}
        assert items[201]["amount"] == 22.5, "汇总引用的计入金额按原记录可追溯"
        assert items[202]["amount"] == 8.75

        after = usage_ledger.query_cycle(CYCLE_OPEN)
        for rec in after["records"]:
            assert rec["settlement_status"] == "已结算"
            assert rec["settlement_ref"] == CYCLE_OPEN
        assert after["summary"]["total_amount"] == 31.25

    # ==========================================================
    # 9. 与用量计量服务集成：record_usage 经在途 WAL 落账
    # ==========================================================

    def test_record_usage_integrates_with_ledger_wal(self):
        from app.seed import SEED_ROWS
        from app.store import store
        from app.services.usage_billing import UsageBillingService

        store._tables = {name: [dict(row) for row in rows]
                         for name, rows in SEED_ROWS.items()}
        svc = UsageBillingService()
        cycle = svc._current_billing_cycle()

        svc.record_usage("台账API", "甲方", call_count=3, data_rows=30, amount=1.5)
        svc.record_usage("台账API", "甲方", call_count=2, data_rows=20, amount=0.5)

        # 旧目录镜像累计写一次一单
        records = [r for r in usage_ledger._json_cycle_records(cycle)
                   if r["api_name"] == "台账API"]
        assert len(records) == 1
        assert records[0]["call_count"] == 5
        assert records[0]["amount"] == 2.0
        # 在途请求全部应用，无滞留
        assert usage_ledger.inflight_status().get("pending", 0) == 0
        assert usage_ledger.inflight_status().get("applied", 0) >= 2
        # 未迁移账期查询走旧目录，本接口写入的金额一致（当前账期可能与夹具同月，按单过滤）
        query = usage_ledger.query_cycle(cycle)
        assert query["source"] == "json"
        mine = [r for r in query["records"] if r["api_name"] == "台账API"]
        assert len(mine) == 1
        assert mine[0]["amount"] == 2.0
        assert mine[0]["call_count"] == 5


if __name__ == "__main__":
    import traceback

    klass = TestUsageLedgerMigration
    names = sorted(n for n in dir(klass) if n.startswith("test_"))
    failures = 0
    for name in names:
        inst = klass()
        try:
            inst.setup_method()
            try:
                getattr(inst, name)()
            finally:
                inst.teardown_method()
            print(f"PASS {name}")
        except Exception:
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
    print(f"\n{len(names) - failures}/{len(names)} 通过")
    sys.exit(1 if failures else 0)
