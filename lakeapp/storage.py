from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_batches (
    source TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    cursor INTEGER NOT NULL,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (source, batch_id)
);
CREATE TABLE IF NOT EXISTS source_progress (
    source TEXT PRIMARY KEY,
    cursor INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS facts (
    source TEXT NOT NULL,
    business_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    source_batch TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (source, business_key)
);
CREATE TABLE IF NOT EXISTS manifests (
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (name, version)
);
CREATE TABLE IF NOT EXISTS quarantine_records (
    source TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    partition TEXT,
    business_key TEXT,
    raw_payload TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    reason_detail TEXT NOT NULL,
    state TEXT NOT NULL,
    awaited_ref TEXT,
    last_corrected TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (source, batch_id, record_index)
);
CREATE TABLE IF NOT EXISTS quarantine_replays (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    attempt_no INTEGER NOT NULL,
    input_payload TEXT NOT NULL,
    result TEXT NOT NULL,
    reason_code TEXT,
    reason_detail TEXT,
    fact_version INTEGER,
    attempted_at TEXT NOT NULL,
    UNIQUE (source, batch_id, record_index, attempt_no)
);
CREATE INDEX IF NOT EXISTS idx_quarantine_state ON quarantine_records(state, partition);
CREATE INDEX IF NOT EXISTS idx_quarantine_ref ON quarantine_records(awaited_ref) WHERE awaited_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_replays_key ON quarantine_replays(source, batch_id, record_index);
"""

# 旧库升级：manifests 增加分区列，记录发布门槛对应的分区。
_MIGRATIONS = (
    (
        "manifests",
        "partition",
        "ALTER TABLE manifests ADD COLUMN partition TEXT",
    ),
)


def open_store(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA)
    existing = {row["name"] for row in connection.execute("PRAGMA table_info(manifests)")}
    for _table, column, ddl in _MIGRATIONS:
        if column not in existing:
            connection.execute(ddl)
    connection.commit()
    return connection


@contextmanager
def transaction(connection: sqlite3.Connection):
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def encode(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode(value: str):
    return json.loads(value)
