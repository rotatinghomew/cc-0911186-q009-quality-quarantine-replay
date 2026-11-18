from __future__ import annotations

from .contract import ContractError, REASON_REFERENCE_MISSING, parse_event
from .storage import decode, encode, transaction


# 隔离状态机：
#   隔离   —— 字段/枚举/类型等结构问题，等待源侧补正
#   等待引用 —— 结构合法，但跨表引用的事实尚未到达
#   可重放 —— 已补正或引用已到达，下一次重放可成功
#   已入仓 —— 重放成功，事实已按正常幂等/冲突规则写入
STATE_QUARANTINED = "隔离"
STATE_WAITING_REF = "等待引用"
STATE_REPLAYABLE = "可重放"
STATE_STORED = "已入仓"

# 分区发布门槛：
#   可用 —— 无未解决质量缺口
#   延迟 —— 仍有缺口但策略允许带缺口发布
#   阻断 —— 仍有缺口且策略不允许发布
GATE_AVAILABLE = "可用"
GATE_DELAYED = "延迟"
GATE_BLOCKED = "阻断"

UNRESOLVED_STATES = (STATE_QUARANTINED, STATE_WAITING_REF, STATE_REPLAYABLE)
ALL_STATES = (STATE_QUARANTINED, STATE_WAITING_REF, STATE_REPLAYABLE, STATE_STORED)


def next_state(current: str, dependency_ready: bool, corrected: bool) -> str:
    """隔离记录的领域状态迁移规则（保留基线行为并覆盖引用等待场景）。"""
    if current == STATE_STORED:
        return STATE_STORED
    if current == STATE_QUARANTINED and corrected and dependency_ready:
        return STATE_REPLAYABLE
    if current == STATE_QUARANTINED and not dependency_ready:
        return STATE_WAITING_REF
    if current == STATE_WAITING_REF and dependency_ready:
        return STATE_REPLAYABLE
    if current == STATE_REPLAYABLE and not dependency_ready:
        return STATE_WAITING_REF
    return current


def partition_gate(unresolved: int, policy: str) -> str:
    """根据未解决质量缺口数量与发布策略给出分区门槛结论。"""
    if unresolved == 0:
        return GATE_AVAILABLE
    return GATE_DELAYED if policy == "allow_delay" else GATE_BLOCKED


def partition_of(record: dict) -> str | None:
    """从业务时间提取业务日期分区；无法提取时返回 None（记录本身会被隔离）。"""
    occurred_at = record.get("occurred_at")
    if isinstance(occurred_at, str) and len(occurred_at) >= 10:
        return occurred_at[:10]
    return None


def reference_exists(connection, ref: str) -> bool:
    """跨表引用以业务键指向事实表，引用事实到达后等待方可解除。"""
    row = connection.execute("SELECT 1 FROM facts WHERE business_key=? LIMIT 1", (ref,)).fetchone()
    return row is not None


def record_failure(connection, source: str, batch_id: str, record_index: int,
                   raw: dict, reason_code: str, reason_detail: str) -> None:
    """把未通过的原始行落为可追踪的隔离记录，重复登记保持幂等。

    已通过补正入仓的记录保持终态，不会因整批重跑被重新打回隔离。
    """
    existing = _get_quarantine(connection, source, batch_id, record_index)
    if existing is not None and existing["state"] == STATE_STORED:
        return
    state = STATE_WAITING_REF if reason_code == REASON_REFERENCE_MISSING else STATE_QUARANTINED
    awaited_ref = raw.get("ref") if reason_code == REASON_REFERENCE_MISSING else None
    with transaction(connection):
        connection.execute(
            "INSERT INTO quarantine_records("
            "source,batch_id,record_index,partition,business_key,raw_payload,"
            "reason_code,reason_detail,state,awaited_ref,attempts,created_at,updated_at"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,0,datetime('now'),datetime('now')) "
            "ON CONFLICT(source,batch_id,record_index) DO UPDATE SET "
            "reason_code=excluded.reason_code,reason_detail=excluded.reason_detail,"
            "state=excluded.state,awaited_ref=excluded.awaited_ref,updated_at=datetime('now')",
            (source, batch_id, record_index, partition_of(raw), raw.get("business_key"),
             encode(raw), reason_code, reason_detail, state, awaited_ref),
        )


def list_quarantine(connection, source: str | None = None,
                    state: str | None = None, partition: str | None = None) -> list[dict]:
    """查询隔离记录，原始行与失败缘由一并返回，便于运营追踪。"""
    sql = "SELECT * FROM quarantine_records WHERE 1=1"
    params: list = []
    if source is not None:
        sql += " AND source=?"
        params.append(source)
    if state is not None:
        sql += " AND state=?"
        params.append(state)
    if partition is not None:
        sql += " AND partition=?"
        params.append(partition)
    sql += " ORDER BY source,batch_id,record_index"
    rows = []
    for row in connection.execute(sql, params):
        item = dict(row)
        item["raw_payload"] = decode(item["raw_payload"])
        if item["last_corrected"]:
            item["last_corrected"] = decode(item["last_corrected"])
        rows.append(item)
    return rows


def _get_quarantine(connection, source: str, batch_id: str, record_index: int):
    return connection.execute(
        "SELECT * FROM quarantine_records WHERE source=? AND batch_id=? AND record_index=?",
        (source, batch_id, record_index),
    ).fetchone()


def _refresh_references(connection) -> int:
    """引用刷新内核：假定调用方已持有事务。"""
    changed = 0
    rows = connection.execute(
        "SELECT source,batch_id,record_index,awaited_ref FROM quarantine_records "
        "WHERE state=? AND awaited_ref IS NOT NULL",
        (STATE_WAITING_REF,),
    ).fetchall()
    for row in rows:
        if reference_exists(connection, row["awaited_ref"]):
            connection.execute(
                "UPDATE quarantine_records SET state=?,updated_at=datetime('now') "
                "WHERE source=? AND batch_id=? AND record_index=?",
                (STATE_REPLAYABLE, row["source"], row["batch_id"], row["record_index"]),
            )
            changed += 1
    return changed


def refresh_references(connection) -> int:
    """新事实到达后，解除已满足的引用等待；引用仍缺失时保持等待，绝不转为成功。"""
    with transaction(connection):
        return _refresh_references(connection)


def replay_quarantine(connection, source: str, batch_id: str, record_index: int,
                      corrected: dict | None = None) -> dict:
    """对单条隔离记录做补正重放，只重新处理受影响的数据。

    corrected 为源侧补正后的完整记录；为空时使用最近一次补正或原始行。
    每次尝试都写入重放历史；成功时复用事实表正常的写入/冲突/版本规则。
    """
    # 延迟导入避免 storage -> quarantine -> derive 的循环依赖。
    from .derive import apply_one

    with transaction(connection):
        row = _get_quarantine(connection, source, batch_id, record_index)
        if row is None:
            raise KeyError("隔离记录不存在")
        attempt_no = row["attempts"] + 1
        candidate = corrected if corrected is not None else (
            decode(row["last_corrected"]) if row["last_corrected"] else decode(row["raw_payload"])
        )

        def record_attempt(result, reason_code=None, reason_detail=None, fact_version=None):
            connection.execute(
                "INSERT INTO quarantine_replays("
                "source,batch_id,record_index,attempt_no,input_payload,result,"
                "reason_code,reason_detail,fact_version,attempted_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,datetime('now'))",
                (source, batch_id, record_index, attempt_no, encode(candidate), result,
                 reason_code, reason_detail, fact_version),
            )

        # 已入仓且没有任何新补正可应用时，重放为幂等空操作，不产生重复事实写入。
        if row["state"] == STATE_STORED and corrected is None and row["last_corrected"] is None:
            record_attempt("noop")
            return {"result": "noop", "state": STATE_STORED, "attempt_no": attempt_no}

        try:
            event = parse_event(
                source, batch_id, _cursor_of(connection, source, batch_id), candidate
            )
        except ContractError as exc:
            # 补正后结构仍不合法：结构问题保持隔离，引用等待被打回隔离。
            new_state = (STATE_QUARANTINED if row["state"] == STATE_WAITING_REF
                         else row["state"])
            if new_state == STATE_STORED:
                new_state = STATE_QUARANTINED
            connection.execute(
                "UPDATE quarantine_records SET reason_code=?,reason_detail=?,state=?,"
                "last_corrected=?,attempts=attempts+1,updated_at=datetime('now') "
                "WHERE source=? AND batch_id=? AND record_index=?",
                (exc.reason_code, exc.reason_detail, new_state,
                 encode(candidate), source, batch_id, record_index),
            )
            record_attempt("failed", exc.reason_code, exc.reason_detail)
            return {"result": "failed", "state": new_state,
                    "reason_code": exc.reason_code, "reason_detail": exc.reason_detail,
                    "attempt_no": attempt_no}

        if event.ref is not None and not reference_exists(connection, event.ref):
            new_state = next_state(row["state"], dependency_ready=False, corrected=corrected is not None)
            connection.execute(
                "UPDATE quarantine_records SET reason_code=?,reason_detail=?,state=?,awaited_ref=?,"
                "last_corrected=?,partition=?,business_key=?,attempts=attempts+1,updated_at=datetime('now') "
                "WHERE source=? AND batch_id=? AND record_index=?",
                (REASON_REFERENCE_MISSING, "跨表引用未到达: " + event.ref, new_state, event.ref,
                 encode(candidate), partition_of(candidate), event.business_key,
                 source, batch_id, record_index),
            )
            record_attempt("waiting", REASON_REFERENCE_MISSING, "跨表引用未到达: " + event.ref)
            return {"result": "waiting", "state": new_state,
                    "reason_code": REASON_REFERENCE_MISSING,
                    "reason_detail": "跨表引用未到达: " + event.ref, "attempt_no": attempt_no}

        fact_version = apply_one(connection, event)
        connection.execute(
            "UPDATE quarantine_records SET state=?,reason_code=?,reason_detail=?,"
            "last_corrected=?,partition=?,business_key=?,awaited_ref=NULL,"
            "attempts=attempts+1,updated_at=datetime('now') "
            "WHERE source=? AND batch_id=? AND record_index=?",
            (STATE_STORED, "resolved", "重放成功入仓", encode(candidate),
             partition_of(candidate), event.business_key, source, batch_id, record_index),
        )
        record_attempt("stored", fact_version=fact_version)

        # 同一事务内顺带解除其他已满足的引用等待。
        _refresh_references(connection)

    return {"result": "stored", "state": STATE_STORED,
            "fact_version": fact_version, "attempt_no": attempt_no}


def _cursor_of(connection, source: str, batch_id: str) -> int:
    row = connection.execute(
        "SELECT cursor FROM raw_batches WHERE source=? AND batch_id=?", (source, batch_id)
    ).fetchone()
    return row["cursor"] if row else 0


def replay_history(connection, source: str, batch_id: str, record_index: int) -> list[dict]:
    """返回一条隔离记录的每次补正尝试，说明事实如何被逐次改变。"""
    rows = connection.execute(
        "SELECT * FROM quarantine_replays WHERE source=? AND batch_id=? AND record_index=? "
        "ORDER BY attempt_no",
        (source, batch_id, record_index),
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["input_payload"] = decode(item["input_payload"])
        result.append(item)
    return result


def unresolved_count(connection, partition: object = ...) -> int:
    """统计尚未解决的质量缺口；可按业务日期分区过滤。"""
    if partition is ...:
        row = connection.execute(
            "SELECT COUNT(*) AS n FROM quarantine_records WHERE state IN (?,?,?)",
            UNRESOLVED_STATES,
        ).fetchone()
    else:
        row = connection.execute(
            "SELECT COUNT(*) AS n FROM quarantine_records WHERE state IN (?,?,?) AND partition IS ?",
            (*UNRESOLVED_STATES, partition),
        ).fetchone()
    return row["n"]


def partition_quality(connection, partition: str, policy: str = "strict") -> dict:
    """给出分区视角的质量缺口与发布门槛结论。"""
    totals = dict(connection.execute(
        "SELECT state,COUNT(*) AS n FROM quarantine_records WHERE partition=? GROUP BY state",
        (partition,),
    ).fetchall())
    unresolved = sum(totals.get(state, 0) for state in UNRESOLVED_STATES)
    by_reason = {
        row["reason_code"]: row["n"]
        for row in connection.execute(
            "SELECT reason_code,COUNT(*) AS n FROM quarantine_records "
            "WHERE partition=? AND state IN (?,?,?) GROUP BY reason_code",
            (partition, *UNRESOLVED_STATES),
        )
    }
    return {
        "partition": partition,
        "unresolved": unresolved,
        "by_state": {state: totals.get(state, 0) for state in ALL_STATES},
        "by_reason": by_reason,
        "gate": partition_gate(unresolved, policy),
        "policy": policy,
    }


def trace_record(connection, source: str, batch_id: str, record_index: int) -> dict:
    """从原始行、失败缘由到每次补正如何改变事实的完整追踪视图。"""
    row = _get_quarantine(connection, source, batch_id, record_index)
    if row is None:
        raise KeyError("隔离记录不存在")
    item = dict(row)
    item["raw_payload"] = decode(item["raw_payload"])
    item["last_corrected"] = decode(item["last_corrected"]) if item["last_corrected"] else None
    item["replays"] = replay_history(connection, source, batch_id, record_index)
    fact = None
    if item["business_key"]:
        fact_row = connection.execute(
            "SELECT source,business_key,payload,source_batch,version FROM facts "
            "WHERE business_key=? ORDER BY version DESC LIMIT 1",
            (item["business_key"],),
        ).fetchone()
        if fact_row:
            fact = dict(fact_row)
            fact["payload"] = decode(fact["payload"])
    item["current_fact"] = fact
    return item
