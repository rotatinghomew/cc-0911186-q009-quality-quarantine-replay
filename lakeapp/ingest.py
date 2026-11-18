from __future__ import annotations

from .contract import ContractError, Event, REASON_REFERENCE_MISSING, parse_event
from .quarantine import record_failure, reference_exists
from .storage import encode, transaction


def _classify(connection, source: str, batch_id: str, cursor: int,
              records: list[dict]) -> list[tuple[int, Event | None, ContractError | None, dict | None]]:
    """逐条分类：结构合法且引用就绪的产出事件，其余形成可追踪的隔离失败。"""
    classified: list[tuple[int, Event | None, ContractError | None, dict | None]] = []
    for index, raw in enumerate(records):
        try:
            event = parse_event(source, batch_id, cursor, raw)
        except ContractError as exc:
            classified.append((index, None, exc, raw))
            continue
        if event.ref is not None and not reference_exists(connection, event.ref):
            exc = ContractError(REASON_REFERENCE_MISSING, "跨表引用未到达: " + event.ref)
            classified.append((index, None, exc, raw))
            continue
        classified.append((index, event, None, None))
    return classified


def receive(connection, source: str, batch_id: str, cursor: int,
            records: list[dict]) -> list[Event]:
    """接收来源批次。

    原始批次整体保存保证可追溯；未通过约定或引用未到达的行落隔离表，
    只返回可安全入仓的事件。重复批次幂等：已入仓的不重复写、已隔离的不重复登记。
    """
    existing = connection.execute(
        "SELECT payload FROM raw_batches WHERE source=? AND batch_id=?", (source, batch_id)
    ).fetchone()
    payload = {"cursor": cursor, "records": records}
    if existing:
        if existing["payload"] != encode(payload):
            raise ValueError("同一批次内容冲突")
        classified = _classify(connection, source, batch_id, cursor, records)
    else:
        with transaction(connection):
            connection.execute(
                "INSERT INTO raw_batches(source,batch_id,cursor,payload,received_at) "
                "VALUES(?,?,?,?,datetime('now'))",
                (source, batch_id, cursor, encode(payload)),
            )
            connection.execute(
                "INSERT INTO source_progress(source,cursor) VALUES(?,?) "
                "ON CONFLICT(source) DO UPDATE SET cursor=MAX(cursor,excluded.cursor)",
                (source, cursor),
            )
        classified = _classify(connection, source, batch_id, cursor, records)

    events: list[Event] = []
    managed_indexes = {
        row["record_index"]
        for row in connection.execute(
            "SELECT record_index FROM quarantine_records WHERE source=? AND batch_id=?",
            (source, batch_id),
        )
    }
    for index, event, error, raw in classified:
        if event is not None:
            # 重放批次时，已由隔离流程接管的行不再作为普通事件重复入仓。
            if index not in managed_indexes:
                events.append(event)
            continue
        record_failure(connection, source, batch_id, index, raw,
                       error.reason_code, error.reason_detail)
    return events


def progress(connection) -> list[dict]:
    return [dict(row) for row in connection.execute(
        "SELECT source,cursor FROM source_progress ORDER BY source")]
