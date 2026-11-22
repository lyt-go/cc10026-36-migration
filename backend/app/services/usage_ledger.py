"""用量台账：把分散保存的历史用量 JSON 与结算汇总迁入 SQLite，并存期间提供一致读写。

背景：data/usage/ 下按记录分散保存历史用量 JSON（000001.json …），结算汇总存
_settlement_<账期>.json。本模块把两类历史明细（已结/未结）迁入 SQLite 用量台账，
迁移流水线按账期推进：扫描 → 装载 → 校验 → 切换读取，状态落库可断点恢复。

设计要点：
- 原单可追溯：台账保留原明细身份（usage_id=原 JSON 文件名、record_no）、
  结算状态（settlement_status）与汇总引用（settlement_ref / settlement_item）。
- 层次：原记录层 layer='base'（迁移或新单首写），新写入层 layer='delta'
  （切换后对在册记录的增量），确认完成后可按原记录查到应收金额与新写入层。
- 在途请求：写新用量先记 WAL（inflight_request），应用幂等，恢复时重放，
  保证不漏记、不重记。
- 并存一致：数据库与旧目录并存期间，查询与结算只使用同一账期双方相符的
  确认视图；冲突时 reconcile_cycle 给出双方视图对照，结算拒绝放行。
- 查询与结算共用 query_cycle 的同一份台账数据。

测试注入：各阶段函数接受 crash_at 参数，命中即抛 CrashAbort，用于验证
写新用量、生成结算、切换读取三个阶段中止后的恢复结果。
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from app.services import data_manager

# 东八区，与 data_manager 保持一致
TZ = timezone(timedelta(hours=8))

# 迁移流水线阶段：pending → scanning → loading → verifying → switching → done
PHASES = ("pending", "scanning", "loading", "verifying", "switching", "done", "failed")
# 校验不一致时回退装载的最大次数（装载会重扫旧目录，吸收在途新写入）
MAX_LOAD_RETRIES = 10

LAYER_BASE = "base"    # 原记录层
LAYER_DELTA = "delta"  # 新写入层


class CrashAbort(Exception):
    """测试注入的中止异常：模拟流水线在指定阶段崩溃。"""


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


def _data_root() -> Path:
    """每次调用实时读取，便于测试用临时目录隔离。"""
    return Path(data_manager.DATA_ROOT)


def _db_path() -> Path:
    return _data_root() / "ledger" / "usage_ledger.db"


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    _init_schema(conn)
    return conn


@contextmanager
def _tx() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS usage_ledger (
            usage_id INTEGER NOT NULL,
            layer TEXT NOT NULL DEFAULT 'base',
            request_id TEXT NOT NULL DEFAULT '',
            record_no TEXT,
            caller TEXT,
            api_name TEXT,
            call_count INTEGER NOT NULL DEFAULT 0,
            data_rows INTEGER NOT NULL DEFAULT 0,
            amount REAL NOT NULL DEFAULT 0,
            billing_cycle TEXT NOT NULL,
            settlement_status TEXT NOT NULL DEFAULT '计量中',
            settlement_ref TEXT,
            source TEXT NOT NULL,
            migration_batch INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (usage_id, layer, request_id)
        );
        -- 在途请求幂等键：同一 request_id 的增量只允许入账一次
        CREATE UNIQUE INDEX IF NOT EXISTS ux_ledger_request
            ON usage_ledger(request_id) WHERE request_id <> '';
        CREATE INDEX IF NOT EXISTS ix_ledger_cycle ON usage_ledger(billing_cycle);

        CREATE TABLE IF NOT EXISTS settlement_summary (
            billing_cycle TEXT PRIMARY KEY,
            total_amount REAL NOT NULL,
            total_calls INTEGER NOT NULL,
            record_count INTEGER NOT NULL,
            settled_at TEXT NOT NULL,
            auto_settlement INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'confirmed',
            source TEXT NOT NULL,
            migration_batch INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        -- 汇总引用：结算汇总与原始明细的关联，结算时点的计入金额
        CREATE TABLE IF NOT EXISTS settlement_item (
            billing_cycle TEXT NOT NULL,
            usage_id INTEGER NOT NULL,
            record_no TEXT,
            amount REAL NOT NULL,
            call_count INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (billing_cycle, usage_id)
        );

        CREATE TABLE IF NOT EXISTS migration_state (
            billing_cycle TEXT PRIMARY KEY,
            phase TEXT NOT NULL,
            batch_id INTEGER NOT NULL,
            retries INTEGER NOT NULL DEFAULT 0,
            json_record_count INTEGER NOT NULL DEFAULT 0,
            json_total_amount REAL NOT NULL DEFAULT 0,
            db_record_count INTEGER NOT NULL DEFAULT 0,
            db_total_amount REAL NOT NULL DEFAULT 0,
            has_summary INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            updated_at TEXT NOT NULL
        );

        -- 在途请求 WAL：先记日志再应用，恢复时重放，不漏记不重记
        CREATE TABLE IF NOT EXISTS inflight_request (
            request_id TEXT PRIMARY KEY,
            usage_id INTEGER NOT NULL,
            billing_cycle TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            applied_at TEXT
        );
        """
    )


def _maybe_crash(crash_at: str | None, point: str) -> None:
    if crash_at == point:
        raise CrashAbort(f"在阶段「{point}」注入中止")


def _to_int(value: Any) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


# ========== 旧目录（JSON）视图 ==========


def _usage_dir() -> Path:
    path = _data_root() / "usage"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _json_cycle_records(billing_cycle: str) -> list[dict[str, Any]]:
    """读取旧目录中某账期的用量明细，原单身份取 JSON 文件名。"""
    records = []
    for f in sorted(_usage_dir().glob("[0-9]*.json")):
        data = data_manager.read_usage(_to_int(f.stem))
        if not data or data.get("billing_cycle") != billing_cycle:
            continue
        records.append(_normalize_json_record(_to_int(f.stem), data))
    return records


def _normalize_json_record(usage_id: int, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "usage_id": usage_id,
        "record_no": data.get("record_no"),
        "caller": data.get("caller") or data.get("applicant"),
        "api_name": data.get("api_name"),
        "call_count": _to_int(data.get("call_count")),
        "data_rows": _to_int(data.get("data_rows")),
        "amount": _to_float(data.get("amount")),
        "billing_cycle": data.get("billing_cycle"),
        "settlement_status": data.get("settlement_status") or "计量中",
        "source": "json",
    }


def _json_summary_path(billing_cycle: str) -> Path:
    return _usage_dir() / f"_settlement_{billing_cycle}.json"


def _json_cycle_summary(billing_cycle: str) -> dict[str, Any] | None:
    path = _json_summary_path(billing_cycle)
    if not path.exists():
        return None
    return data_manager._read_json(path, None)


# ========== 数据库（台账）视图 ==========


def _db_cycle_layers(billing_cycle: str) -> dict[int, dict[str, Any]]:
    """按原单（usage_id）聚合台账层次：base 为原记录层，delta 为新写入层。"""
    with _tx() as conn:
        rows = conn.execute(
            "SELECT * FROM usage_ledger WHERE billing_cycle=? ORDER BY usage_id, layer, created_at",
            (billing_cycle,),
        ).fetchall()
    grouped: dict[int, dict[str, Any]] = {}
    for row in rows:
        item = grouped.setdefault(row["usage_id"], {"base": None, "deltas": []})
        if row["layer"] == LAYER_BASE:
            item["base"] = dict(row)
        else:
            item["deltas"].append(dict(row))
    return grouped


def _db_cycle_view(billing_cycle: str) -> list[dict[str, Any]]:
    """数据库侧账期视图：应收金额 = 原记录层 + 新写入层合计。"""
    view = []
    for usage_id, item in sorted(_db_cycle_layers(billing_cycle).items()):
        base = item["base"] or {}
        deltas = item["deltas"]
        base_amount = _to_float(base.get("amount"))
        delta_amount = sum(_to_float(d.get("amount")) for d in deltas)
        base_calls = _to_int(base.get("call_count"))
        delta_calls = sum(_to_int(d.get("call_count")) for d in deltas)
        base_rows = _to_int(base.get("data_rows"))
        delta_rows = sum(_to_int(d.get("data_rows")) for d in deltas)
        layers = []
        if base:
            layers.append({
                "layer": LAYER_BASE, "amount": base_amount,
                "call_count": base_calls, "source": base.get("source"),
                "migration_batch": base.get("migration_batch"),
            })
        layers.extend({
            "layer": LAYER_DELTA, "amount": _to_float(d.get("amount")),
            "call_count": _to_int(d.get("call_count")),
            "source": d.get("source"), "request_id": d.get("request_id"),
        } for d in deltas)
        view.append({
            "usage_id": usage_id,
            "record_no": base.get("record_no"),
            "caller": base.get("caller"),
            "api_name": base.get("api_name"),
            "billing_cycle": billing_cycle,
            "call_count": base_calls + delta_calls,
            "data_rows": base_rows + delta_rows,
            "amount": round(base_amount + delta_amount, 6),
            "base_amount": round(base_amount, 6),
            "delta_amount": round(delta_amount, 6),
            "settlement_status": base.get("settlement_status") or "计量中",
            "settlement_ref": base.get("settlement_ref"),
            "source": "db",
            "layers": layers,
        })
    return view


def _db_summary(billing_cycle: str) -> dict[str, Any] | None:
    with _tx() as conn:
        row = conn.execute(
            "SELECT * FROM settlement_summary WHERE billing_cycle=?", (billing_cycle,)
        ).fetchone()
    return dict(row) if row else None


def _db_summary_items(billing_cycle: str) -> list[dict[str, Any]]:
    with _tx() as conn:
        rows = conn.execute(
            "SELECT * FROM settlement_item WHERE billing_cycle=? ORDER BY usage_id",
            (billing_cycle,),
        ).fetchall()
    return [dict(r) for r in rows]


def _get_state(billing_cycle: str) -> dict[str, Any] | None:
    with _tx() as conn:
        row = conn.execute(
            "SELECT * FROM migration_state WHERE billing_cycle=?", (billing_cycle,)
        ).fetchone()
    return dict(row) if row else None


def _set_state(billing_cycle: str, **fields: Any) -> None:
    fields["updated_at"] = _now_iso()
    with _tx() as conn:
        existing = conn.execute(
            "SELECT billing_cycle FROM migration_state WHERE billing_cycle=?", (billing_cycle,)
        ).fetchone()
        if existing:
            assignments = ", ".join(f"{k}=?" for k in fields)
            conn.execute(
                f"UPDATE migration_state SET {assignments} WHERE billing_cycle=?",
                (*fields.values(), billing_cycle),
            )
        else:
            fields.setdefault("phase", "pending")
            fields.setdefault("batch_id", int(time.time() * 1000))
            marks = ", ".join("?" for _ in fields)
            conn.execute(
                f"INSERT INTO migration_state (billing_cycle, {', '.join(fields)})"
                f" VALUES (?, {marks})",
                (billing_cycle, *fields.values()),
            )


def _is_switched(billing_cycle: str) -> bool:
    state = _get_state(billing_cycle)
    return bool(state and state["phase"] == "done")


# ========== 迁移流水线 ==========


def migrate_cycle(billing_cycle: str, crash_at: str | None = None) -> dict[str, Any]:
    """迁移一个账期（已结/未结均可），可断点恢复。

    阶段：pending → scanning → loading → verifying → switching → done。
    每个阶段推进都落库；中止后再次调用即从断点继续。装载阶段会重扫旧目录，
    迁移期间新写入的用量会在下一轮装载被吸收，校验不通过则回退装载。
    """
    state = _get_state(billing_cycle)
    if state is None:
        _set_state(billing_cycle, phase="pending", batch_id=int(time.time() * 1000))
        state = _get_state(billing_cycle)

    while True:
        state = _get_state(billing_cycle)
        phase = state["phase"]
        if phase == "done":
            return migration_status(billing_cycle)
        if phase == "failed":
            return migration_status(billing_cycle)
        if phase == "pending":
            _maybe_crash(crash_at, "scan")
            records = _json_cycle_records(billing_cycle)
            summary = _json_cycle_summary(billing_cycle)
            _set_state(
                billing_cycle,
                phase="scanning",
                json_record_count=len(records),
                json_total_amount=round(sum(r["amount"] for r in records), 6),
                has_summary=1 if summary else 0,
                error=None,
            )
        elif phase == "scanning":
            _set_state(billing_cycle, phase="loading")
        elif phase == "loading":
            _load_cycle(billing_cycle, state["batch_id"], crash_at)
            _set_state(billing_cycle, phase="verifying")
        elif phase == "verifying":
            _maybe_crash(crash_at, "verify")
            diffs = _verify_cycle(billing_cycle)
            if not diffs:
                _set_state(billing_cycle, phase="switching", error=None)
            elif state["retries"] < MAX_LOAD_RETRIES:
                # 与当前记账冲突：回退装载重扫旧目录，吸收在途写入后再校验
                _set_state(billing_cycle, phase="loading", retries=state["retries"] + 1)
            else:
                _set_state(billing_cycle, phase="failed", error=json.dumps(diffs, ensure_ascii=False))
        elif phase == "switching":
            # 切换读取：切换前再校验一次，随后原子翻转到 done
            diffs = _verify_cycle(billing_cycle)
            if diffs:
                _set_state(billing_cycle, phase="loading", retries=state["retries"] + 1)
                continue
            _maybe_crash(crash_at, "switch")
            db_view = _db_cycle_view(billing_cycle)
            _set_state(
                billing_cycle,
                phase="done",
                db_record_count=len(db_view),
                db_total_amount=round(sum(r["amount"] for r in db_view), 6),
                error=None,
            )


def _load_cycle(billing_cycle: str, batch_id: int, crash_at: str | None) -> None:
    """装载：把旧目录明细 upsert 进台账 base 层，保留原明细身份与结算状态。"""
    records = _json_cycle_records(billing_cycle)
    now = _now_iso()
    for index, rec in enumerate(records):
        with _tx() as conn:
            conn.execute(
                """
                INSERT INTO usage_ledger (
                    usage_id, layer, request_id, record_no, caller, api_name,
                    call_count, data_rows, amount, billing_cycle,
                    settlement_status, settlement_ref, source, migration_batch,
                    created_at, updated_at
                ) VALUES (?, 'base', '', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'json_migration', ?, ?, ?)
                ON CONFLICT(usage_id, layer, request_id) DO UPDATE SET
                    record_no=excluded.record_no,
                    caller=excluded.caller,
                    api_name=excluded.api_name,
                    call_count=excluded.call_count,
                    data_rows=excluded.data_rows,
                    amount=excluded.amount,
                    settlement_status=excluded.settlement_status,
                    updated_at=excluded.updated_at
                """,
                (
                    rec["usage_id"], rec["record_no"], rec["caller"], rec["api_name"],
                    rec["call_count"], rec["data_rows"], rec["amount"], billing_cycle,
                    rec["settlement_status"],
                    billing_cycle if rec["settlement_status"] == "已结算" else None,
                    batch_id, now, now,
                ),
            )
        if index == 0:
            _maybe_crash(crash_at, "load")

    summary = _json_cycle_summary(billing_cycle)
    if summary:
        _migrate_summary(billing_cycle, summary, records, batch_id)


def _migrate_summary(billing_cycle: str, summary: dict[str, Any],
                     records: list[dict[str, Any]], batch_id: int) -> None:
    """迁移结算汇总，并建立汇总引用（settlement_item）关联原明细。"""
    now = _now_iso()
    with _tx() as conn:
        conn.execute(
            """
            INSERT INTO settlement_summary (
                billing_cycle, total_amount, total_calls, record_count,
                settled_at, auto_settlement, status, source, migration_batch,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'confirmed', 'json_migration', ?, ?, ?)
            ON CONFLICT(billing_cycle) DO UPDATE SET
                total_amount=excluded.total_amount,
                total_calls=excluded.total_calls,
                record_count=excluded.record_count,
                updated_at=excluded.updated_at
            """,
            (
                billing_cycle,
                _to_float(summary.get("total_amount")),
                _to_int(summary.get("total_calls")),
                _to_int(summary.get("record_count")) or len(records),
                str(summary.get("settled_at") or now),
                1 if summary.get("auto_settlement", True) else 0,
                batch_id, now, now,
            ),
        )
        for rec in records:
            conn.execute(
                """
                INSERT INTO settlement_item (billing_cycle, usage_id, record_no, amount, call_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(billing_cycle, usage_id) DO UPDATE SET
                    record_no=excluded.record_no,
                    amount=excluded.amount,
                    call_count=excluded.call_count
                """,
                (billing_cycle, rec["usage_id"], rec["record_no"], rec["amount"], rec["call_count"], now),
            )
        conn.execute(
            "UPDATE usage_ledger SET settlement_ref=?, updated_at=?"
            " WHERE billing_cycle=? AND layer='base' AND settlement_status='已结算'",
            (billing_cycle, now, billing_cycle),
        )


def _verify_cycle(billing_cycle: str) -> list[dict[str, Any]]:
    """校验：同一账期旧目录与台账逐单对照，返回不一致清单（空为通过）。"""
    json_view = {r["usage_id"]: r for r in _json_cycle_records(billing_cycle)}
    db_view = {r["usage_id"]: r for r in _db_cycle_view(billing_cycle)}
    diffs: list[dict[str, Any]] = []
    for usage_id in sorted(set(json_view) | set(db_view)):
        j = json_view.get(usage_id)
        d = db_view.get(usage_id)
        if j is None:
            diffs.append({"usage_id": usage_id, "kind": "db_only"})
            continue
        if d is None:
            diffs.append({"usage_id": usage_id, "kind": "json_only"})
            continue
        mismatch = {}
        for field in ("call_count", "data_rows", "amount", "settlement_status"):
            jv, dv = j.get(field), d.get(field)
            if field == "amount":
                same = abs(_to_float(jv) - _to_float(dv)) < 1e-9
            else:
                same = jv == dv
            if not same:
                mismatch[field] = {"json": jv, "db": dv}
        if mismatch:
            diffs.append({"usage_id": usage_id, "kind": "mismatch", "fields": mismatch})
    return diffs


def migration_status(billing_cycle: str | None = None) -> dict[str, Any]:
    """迁移进度：单账期或全部账期。"""
    with _tx() as conn:
        if billing_cycle:
            rows = conn.execute(
                "SELECT * FROM migration_state WHERE billing_cycle=?", (billing_cycle,)
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM migration_state ORDER BY billing_cycle").fetchall()
    states = [dict(r) for r in rows]
    if billing_cycle:
        return states[0] if states else {"billing_cycle": billing_cycle, "phase": "未开始"}
    return {"cycles": states}


# ========== 在途写请求（WAL） ==========


def apply_live_write(record: dict[str, Any], increment: dict[str, Any] | None = None,
                     is_new: bool = False, crash_at: str | None = None) -> dict[str, Any]:
    """写新用量：先记在途 WAL，再应用到旧目录与（已切换的）台账。

    record 为最终累计形态（与旧目录 JSON 同构），increment 为本次增量。
    任何阶段中止后，recover_inflight 重放未完成请求：应用幂等，不漏记不重记。
    """
    usage_id = _to_int(record.get("usage_id"))
    billing_cycle = str(record.get("billing_cycle") or "")
    inc = {
        "call_count": _to_int((increment or {}).get("call_count", record.get("call_count"))),
        "data_rows": _to_int((increment or {}).get("data_rows", record.get("data_rows"))),
        "amount": _to_float((increment or {}).get("amount", record.get("amount"))),
    }
    payload = {
        "usage_id": usage_id,
        "record_no": record.get("record_no"),
        "caller": record.get("caller") or record.get("applicant"),
        "api_name": record.get("api_name"),
        "billing_cycle": billing_cycle,
        "settlement_status": record.get("settlement_status") or "计量中",
        "cum_call_count": _to_int(record.get("call_count")),
        "cum_data_rows": _to_int(record.get("data_rows")),
        "cum_amount": _to_float(record.get("amount")),
        "inc_call_count": inc["call_count"],
        "inc_data_rows": inc["data_rows"],
        "inc_amount": inc["amount"],
        "is_new": bool(is_new),
        # 原单完整形态：旧目录镜像按原样覆盖写，保留调用方原有字段
        "mirror": dict(record),
    }
    request_id = uuid.uuid4().hex
    now = _now_iso()
    with _tx() as conn:
        conn.execute(
            "INSERT INTO inflight_request (request_id, usage_id, billing_cycle, payload, status, created_at)"
            " VALUES (?, ?, ?, ?, 'pending', ?)",
            (request_id, usage_id, billing_cycle, json.dumps(payload, ensure_ascii=False), now),
        )
    _maybe_crash(crash_at, "write_journal")

    _apply_inflight(request_id, payload)
    _maybe_crash(crash_at, "write_apply")

    _mark_inflight_applied(request_id)
    return {"request_id": request_id, "usage_id": usage_id, "billing_cycle": billing_cycle,
            "applied": True}


def _apply_inflight(request_id: str, payload: dict[str, Any]) -> None:
    """应用在途请求：旧目录镜像（累计覆盖，幂等）+ 台账入账（按 request_id 幂等）。"""
    usage_id = _to_int(payload.get("usage_id"))
    billing_cycle = str(payload.get("billing_cycle") or "")
    # 1) 旧目录镜像：按原单完整形态整体覆盖（幂等），保留调用方原有字段
    mirror = dict(payload.get("mirror") or {})
    mirror.setdefault("usage_id", usage_id)
    mirror.setdefault("billing_cycle", billing_cycle)
    data_manager.write_usage(usage_id, mirror)
    # 2) 台账入账：仅在该账期已切换读取后；未切换账期由迁移装载重扫旧目录吸收
    if not _is_switched(billing_cycle):
        return
    now = _now_iso()
    with _tx() as conn:
        if payload.get("is_new"):
            conn.execute(
                """
                INSERT INTO usage_ledger (
                    usage_id, layer, request_id, record_no, caller, api_name,
                    call_count, data_rows, amount, billing_cycle,
                    settlement_status, settlement_ref, source, migration_batch,
                    created_at, updated_at
                ) VALUES (?, 'base', '', ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'live_write', NULL, ?, ?)
                ON CONFLICT(usage_id, layer, request_id) DO UPDATE SET
                    record_no=excluded.record_no,
                    caller=excluded.caller,
                    api_name=excluded.api_name,
                    call_count=excluded.call_count,
                    data_rows=excluded.data_rows,
                    amount=excluded.amount,
                    settlement_status=excluded.settlement_status,
                    updated_at=excluded.updated_at
                """,
                (
                    usage_id, payload.get("record_no"), payload.get("caller"), payload.get("api_name"),
                    _to_int(payload.get("cum_call_count")), _to_int(payload.get("cum_data_rows")),
                    _to_float(payload.get("cum_amount")), billing_cycle,
                    payload.get("settlement_status") or "计量中", now, now,
                ),
            )
        else:
            # 新写入层：增量入账，request_id 唯一约束保证重放不重记
            conn.execute(
                """
                INSERT OR IGNORE INTO usage_ledger (
                    usage_id, layer, request_id, record_no, caller, api_name,
                    call_count, data_rows, amount, billing_cycle,
                    settlement_status, settlement_ref, source, migration_batch,
                    created_at, updated_at
                ) VALUES (?, 'delta', ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'live_write', NULL, ?, ?)
                """,
                (
                    usage_id, request_id, payload.get("record_no"), payload.get("caller"),
                    payload.get("api_name"),
                    _to_int(payload.get("inc_call_count")), _to_int(payload.get("inc_data_rows")),
                    _to_float(payload.get("inc_amount")), billing_cycle,
                    payload.get("settlement_status") or "计量中", now, now,
                ),
            )


def _mark_inflight_applied(request_id: str) -> None:
    with _tx() as conn:
        conn.execute(
            "UPDATE inflight_request SET status='applied', applied_at=? WHERE request_id=?",
            (_now_iso(), request_id),
        )


def recover_inflight() -> dict[str, Any]:
    """恢复流程：重放全部在途请求，保证不漏记；应用幂等，保证不重记。"""
    with _tx() as conn:
        rows = conn.execute(
            "SELECT * FROM inflight_request WHERE status='pending' ORDER BY created_at"
        ).fetchall()
    replayed = []
    for row in rows:
        payload = json.loads(row["payload"])
        _apply_inflight(row["request_id"], payload)
        _mark_inflight_applied(row["request_id"])
        replayed.append({"request_id": row["request_id"], "usage_id": row["usage_id"],
                         "billing_cycle": row["billing_cycle"]})
    return {"replayed": len(replayed), "requests": replayed}


def inflight_status() -> dict[str, Any]:
    with _tx() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM inflight_request GROUP BY status"
        ).fetchall()
    return {row["status"]: row["n"] for row in rows}


# ========== 查询与对账（同一账期确认视图） ==========


def query_cycle(billing_cycle: str) -> dict[str, Any]:
    """同一账期的确认视图，查询与账期结算共用这一份台账数据。

    - 未迁移：旧目录为权威；
    - 迁移中（并存期）：只返回双方相符的确认结果，冲突单列；
    - 已切换：台账为权威，可按原记录查到应收金额与新写入层。
    """
    state = _get_state(billing_cycle)
    if state is None:
        source = "json"
    elif state["phase"] == "done":
        source = "db"
    else:
        source = "dual"

    json_view = _json_cycle_records(billing_cycle)
    db_view = _db_cycle_view(billing_cycle)
    conflicts = _verify_cycle(billing_cycle) if source != "json" else []

    if source == "db":
        records = db_view
    elif source == "dual":
        db_by_id = {r["usage_id"]: r for r in db_view}
        conflict_ids = {d["usage_id"] for d in conflicts}
        records = [db_by_id[uid] for uid in sorted(db_by_id) if uid not in conflict_ids]
    else:
        records = json_view

    return {
        "billing_cycle": billing_cycle,
        "source": source,
        "phase": state["phase"] if state else "未开始",
        "records": records,
        "record_count": len(records),
        "total_amount": round(sum(_to_float(r.get("amount")) for r in records), 6),
        "total_calls": sum(_to_int(r.get("call_count")) for r in records),
        "conflicts": conflicts,
        "summary": _db_summary(billing_cycle) or _json_cycle_summary(billing_cycle),
    }


def reconcile_cycle(billing_cycle: str) -> dict[str, Any]:
    """双方对照：旧目录视图与台账视图并排，列出逐单差异与合计差异。"""
    json_view = _json_cycle_records(billing_cycle)
    db_view = _db_cycle_view(billing_cycle)
    diffs = _verify_cycle(billing_cycle)
    json_total = round(sum(r["amount"] for r in json_view), 6)
    db_total = round(sum(r["amount"] for r in db_view), 6)
    state = _get_state(billing_cycle)
    return {
        "billing_cycle": billing_cycle,
        "phase": state["phase"] if state else "未开始",
        "consistent": not diffs,
        "json_view": {
            "record_count": len(json_view),
            "total_amount": json_total,
            "total_calls": sum(r["call_count"] for r in json_view),
            "records": json_view,
        },
        "db_view": {
            "record_count": len(db_view),
            "total_amount": db_total,
            "total_calls": sum(r["call_count"] for r in db_view),
            "records": db_view,
        },
        "amount_diff": round(json_total - db_total, 6),
        "conflicts": diffs,
        "json_summary": _json_cycle_summary(billing_cycle),
        "db_summary": _db_summary(billing_cycle),
    }


# ========== 账期结算（与查询共用台账数据） ==========


def settle_cycle(billing_cycle: str, auto_settlement: bool = True,
                 crash_at: str | None = None) -> dict[str, Any]:
    """生成结算：基于 query_cycle 的确认视图，幂等，可断点恢复。

    已结账期（汇总已迁移）确认一致后原样返回；未结账期生成结算汇总、
    建立汇总引用并把明细标记为已结算。并存期间若双方视图存在冲突，
    结算拒绝放行并附上双方视图供对照。
    """
    view = query_cycle(billing_cycle)
    if view["source"] == "dual" and view["conflicts"]:
        # 并存期间：结算只能看到双方相符的确认结果，冲突拒绝放行并附双方视图
        return {
            "ok": False,
            "error": "账期存在未对齐明细，结算中止",
            "billing_cycle": billing_cycle,
            "conflicts": view["conflicts"],
            "views": reconcile_cycle(billing_cycle),
        }

    records = view["records"]
    total_amount = round(sum(_to_float(r.get("amount")) for r in records), 6)
    total_calls = sum(_to_int(r.get("call_count")) for r in records)
    record_count = len(records)

    # 幂等：已确认的汇总与本次确认视图一致则直接确认返回
    for existing in (_db_summary(billing_cycle), _json_cycle_summary(billing_cycle)):
        if existing and (
            abs(_to_float(existing.get("total_amount")) - total_amount) > 1e-9
            or _to_int(existing.get("total_calls")) != total_calls
            or _to_int(existing.get("record_count")) != record_count
        ):
            return {
                "ok": False,
                "error": "既有结算汇总与当前确认视图不一致",
                "billing_cycle": billing_cycle,
                "existing_summary": existing,
                "current_view": {"total_amount": total_amount, "total_calls": total_calls,
                                 "record_count": record_count},
            }

    now = _now_iso()
    existing_db = _db_summary(billing_cycle)
    settled_at = str(existing_db.get("settled_at")) if existing_db else now
    with _tx() as conn:
        conn.execute(
            """
            INSERT INTO settlement_summary (
                billing_cycle, total_amount, total_calls, record_count,
                settled_at, auto_settlement, status, source, migration_batch,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'confirmed', 'ledger_settle', NULL, ?, ?)
            ON CONFLICT(billing_cycle) DO UPDATE SET
                total_amount=excluded.total_amount,
                total_calls=excluded.total_calls,
                record_count=excluded.record_count,
                updated_at=excluded.updated_at
            """,
            (billing_cycle, total_amount, total_calls, record_count,
             settled_at, 1 if auto_settlement else 0, now, now),
        )
        for rec in records:
            conn.execute(
                """
                INSERT INTO settlement_item (billing_cycle, usage_id, record_no, amount, call_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(billing_cycle, usage_id) DO UPDATE SET
                    record_no=excluded.record_no,
                    amount=excluded.amount,
                    call_count=excluded.call_count
                """,
                (billing_cycle, rec["usage_id"], rec.get("record_no"),
                 _to_float(rec.get("amount")), _to_int(rec.get("call_count")), now),
            )
        conn.execute(
            "UPDATE usage_ledger SET settlement_status='已结算', settlement_ref=?, updated_at=?"
            " WHERE billing_cycle=? AND layer='base'",
            (billing_cycle, now, billing_cycle),
        )
    _maybe_crash(crash_at, "settle_db")

    # 旧目录镜像：并存期间保持两边一致，覆盖写幂等
    data_manager._write_json(_json_summary_path(billing_cycle), {
        "billing_cycle": billing_cycle,
        "total_amount": total_amount,
        "total_calls": total_calls,
        "record_count": record_count,
        "settled_at": settled_at,
        "auto_settlement": auto_settlement,
    })
    for index, rec in enumerate(_json_cycle_records(billing_cycle)):
        data = data_manager.read_usage(rec["usage_id"])
        if not data:
            continue
        data["settlement_status"] = "已结算"
        data["_settled_at"] = now
        data_manager.write_usage(rec["usage_id"], data)
        if index == 0:
            _maybe_crash(crash_at, "settle_json")

    return {
        "ok": True,
        "billing_cycle": billing_cycle,
        "total_amount": total_amount,
        "total_calls": total_calls,
        "record_count": record_count,
        "settled_at": settled_at,
        "auto_settlement": auto_settlement,
        "source": view["source"],
        "summary": _db_summary(billing_cycle),
        "items": _db_summary_items(billing_cycle),
    }
