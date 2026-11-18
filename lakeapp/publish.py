from __future__ import annotations

from .quarantine import (
    GATE_AVAILABLE,
    GATE_BLOCKED,
    GATE_DELAYED,
    partition_gate,
    unresolved_count,
)
from .storage import decode, encode, transaction

# 清单状态：就绪可发布 / 带缺口延迟发布 / 被门槛阻断（只登记不开放读取）。
STATUS_READY = "ready"
STATUS_DELAYED = "delayed"
STATUS_BLOCKED = "blocked"

_GATE_TO_STATUS = {
    GATE_AVAILABLE: STATUS_READY,
    GATE_DELAYED: STATUS_DELAYED,
    GATE_BLOCKED: STATUS_BLOCKED,
}


def publish(connection, name: str, version: int, rows: list[dict],
            partition: str | None = None, policy: str = "strict") -> dict:
    """按分区质量门槛登记版本。

    - 无未解决缺口：ready，可正常发布读取；
    - 有缺口且策略 allow_delay：delayed，版本带缺口延迟发布并标注缺口；
    - 有缺口且策略 strict：blocked，只登记阻断记录，不开放读取。
    同一 (name, version) 重复发布保持冲突报错，不静默覆盖既有版本。
    """
    unresolved = unresolved_count(connection, partition)
    gate = partition_gate(unresolved, policy)
    status = _GATE_TO_STATUS[gate]
    payload = {
        "rows": rows,
        "row_count": len(rows),
        "partition": partition,
        "policy": policy,
        "quality_gate": gate,
        "unresolved": unresolved,
    }
    with transaction(connection):
        connection.execute(
            "INSERT INTO manifests(name,version,payload,status,partition) VALUES(?,?,?,?,?)",
            (name, version, encode(payload), status, partition),
        )
    return {"name": name, "version": version, "status": status,
            "gate": gate, "unresolved": unresolved}


def read_manifest(connection, name: str, version: int) -> dict:
    """基线读取入口：只有就绪版本可读，阻断/延迟版本显式拒绝。"""
    row = connection.execute(
        "SELECT payload,status FROM manifests WHERE name=? AND version=?", (name, version)
    ).fetchone()
    if not row or row["status"] != STATUS_READY:
        raise KeyError("版本不可读")
    return decode(row["payload"])


def read_version(connection, name: str, version: int) -> dict:
    """带发布状态的读取入口，DWS 可据此判断分区可用、延迟还是受阻。"""
    row = connection.execute(
        "SELECT payload,status,partition FROM manifests WHERE name=? AND version=?",
        (name, version),
    ).fetchone()
    if not row:
        raise KeyError("版本不存在")
    payload = decode(row["payload"])
    return {"status": row["status"], "partition": row["partition"], "manifest": payload}
