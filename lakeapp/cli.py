from __future__ import annotations

import argparse
import json

from .derive import apply_events, facts
from .ingest import progress, receive
from .publish import publish, read_manifest, read_version
from .quarantine import (
    list_quarantine,
    partition_quality,
    replay_history,
    replay_quarantine,
    trace_record,
)
from .storage import open_store


def _load(payload: str | None) -> dict:
    return json.loads(payload or "{}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="本地分层数据服务")
    parser.add_argument("database")
    parser.add_argument(
        "action",
        choices=[
            "receive", "facts", "progress", "publish", "read",
            "quarantine", "replay", "replays", "gate", "trace", "version",
        ],
    )
    parser.add_argument("payload", nargs="?")
    args = parser.parse_args(argv)
    connection = open_store(args.database)
    try:
        if args.action == "receive":
            data = _load(args.payload)
            events = receive(connection, data["source"], data["batch_id"],
                             data["cursor"], data["records"])
            print(json.dumps(
                {"received": len(events), "changed": apply_events(connection, events)},
                ensure_ascii=False))
        elif args.action == "facts":
            data = _load(args.payload)
            print(json.dumps(facts(connection, data.get("partition")), ensure_ascii=False))
        elif args.action == "progress":
            print(json.dumps(progress(connection), ensure_ascii=False))
        elif args.action == "publish":
            data = _load(args.payload)
            result = publish(
                connection, data["name"], data["version"],
                facts(connection, data.get("partition")),
                partition=data.get("partition"),
                policy=data.get("policy", "strict"),
            )
            print(json.dumps(result, ensure_ascii=False))
        elif args.action == "read":
            data = _load(args.payload)
            print(json.dumps(
                read_manifest(connection, data["name"], data["version"]),
                ensure_ascii=False))
        elif args.action == "version":
            data = _load(args.payload)
            print(json.dumps(
                read_version(connection, data["name"], data["version"]),
                ensure_ascii=False))
        elif args.action == "quarantine":
            data = _load(args.payload)
            print(json.dumps(list_quarantine(
                connection, data.get("source"), data.get("state"), data.get("partition")
            ), ensure_ascii=False))
        elif args.action == "replay":
            data = _load(args.payload)
            print(json.dumps(replay_quarantine(
                connection, data["source"], data["batch_id"], data["record_index"],
                data.get("corrected"),
            ), ensure_ascii=False))
        elif args.action == "replays":
            data = _load(args.payload)
            print(json.dumps(replay_history(
                connection, data["source"], data["batch_id"], data["record_index"]
            ), ensure_ascii=False))
        elif args.action == "gate":
            data = _load(args.payload)
            print(json.dumps(partition_quality(
                connection, data["partition"], data.get("policy", "strict")
            ), ensure_ascii=False))
        else:  # trace
            data = _load(args.payload)
            print(json.dumps(trace_record(
                connection, data["source"], data["batch_id"], data["record_index"]
            ), ensure_ascii=False))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
