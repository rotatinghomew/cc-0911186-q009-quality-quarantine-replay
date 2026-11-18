from __future__ import annotations

from .contract import Event
from .storage import decode, encode, transaction


def apply_one(connection, event: Event) -> int:
    """单条事实写入，正常入仓与隔离重放共用同一条幂等/冲突路径。

    返回该业务键写入后的事实版本号；retract 成功删除时返回 0。
    """
    if event.kind == "retract":
        connection.execute(
            "DELETE FROM facts WHERE source=? AND business_key=?",
            (event.source, event.business_key),
        )
        return 0
    row = connection.execute(
        "SELECT version FROM facts WHERE source=? AND business_key=?",
        (event.source, event.business_key),
    ).fetchone()
    next_version = (row["version"] + 1) if row else 1
    connection.execute(
        "INSERT INTO facts(source,business_key,payload,source_batch,version) VALUES(?,?,?,?,1) "
        "ON CONFLICT(source,business_key) DO UPDATE SET payload=excluded.payload,"
        "source_batch=excluded.source_batch,version=facts.version+1",
        (event.source, event.business_key,
         encode({"value": event.value, "occurred_at": event.occurred_at,
                 "ref": event.ref}),
         event.batch_id),
    )
    return next_version


def apply_events(connection, events: list[Event]) -> int:
    changed = 0
    with transaction(connection):
        for event in events:
            apply_one(connection, event)
            changed += 1
        # 新事实可能恰好补齐其他记录等待的跨表引用（复用同一事务内核）。
        from .quarantine import _refresh_references

        _refresh_references(connection)
    return changed


def facts(connection, partition: str | None = None) -> list[dict]:
    """读取事实；可按业务日期（occurred_at 前 10 位）分区过滤。"""
    if partition is None:
        rows = connection.execute(
            "SELECT source,business_key,payload,source_batch,version FROM facts "
            "ORDER BY source,business_key"
        )
    else:
        rows = connection.execute(
            "SELECT source,business_key,payload,source_batch,version FROM facts "
            "WHERE substr(json_extract(payload,'$.occurred_at'),1,10)=? "
            "ORDER BY source,business_key",
            (partition,),
        )
    result = []
    for row in rows:
        item = dict(row)
        item["payload"] = decode(item["payload"])
        result.append(item)
    return result
