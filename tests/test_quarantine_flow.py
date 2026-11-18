import unittest
from pathlib import Path
import tempfile

from lakeapp.contract import (
    REASON_MISSING_FIELD,
    REASON_REFERENCE_MISSING,
    REASON_UNKNOWN_ENUM,
)
from lakeapp.derive import apply_events, facts
from lakeapp.ingest import receive
from lakeapp.publish import (
    STATUS_BLOCKED,
    STATUS_DELAYED,
    STATUS_READY,
    publish,
    read_manifest,
    read_version,
)
from lakeapp.quarantine import (
    GATE_AVAILABLE,
    GATE_BLOCKED,
    GATE_DELAYED,
    STATE_QUARANTINED,
    STATE_REPLAYABLE,
    STATE_STORED,
    STATE_WAITING_REF,
    list_quarantine,
    next_state,
    partition_gate,
    partition_quality,
    replay_history,
    replay_quarantine,
    trace_record,
    unresolved_count,
)
from lakeapp.storage import open_store

PART = "2026-11-01"


def good(key="member-1", **over):
    row = {"business_key": key, "kind": "upsert", "value": 3,
           "occurred_at": PART + "T09:00:00"}
    row.update(over)
    return row


class QuarantineFlowTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.connection = open_store(Path(self.tempdir.name) / "data.db")

    def tearDown(self):
        self.connection.close()
        self.tempdir.cleanup()

    def receive_batch(self, records, batch="batch-1", source="enrollment", cursor=1):
        events = receive(self.connection, source, batch, cursor, records)
        return apply_events(self.connection, events)

    def quarantine(self, index=0):
        return list_quarantine(self.connection)[index]

    def test_missing_field_is_quarantined_not_dropped(self):
        bad = {"kind": "upsert", "value": 3, "occurred_at": PART + "T09:00:00"}
        changed = self.receive_batch([good(), bad])
        self.assertEqual(changed, 1)  # 只有合法行入仓
        self.assertEqual(len(facts(self.connection)), 1)
        q = self.quarantine()
        self.assertEqual(q["state"], STATE_QUARANTINED)
        self.assertEqual(q["reason_code"], REASON_MISSING_FIELD)
        self.assertEqual(q["partition"], PART)
        self.assertEqual(q["raw_payload"], bad)
        self.assertEqual(q["attempts"], 0)

    def test_unknown_enum_is_quarantined_with_reason(self):
        self.receive_batch([good(kind="freeze")])
        q = self.quarantine()
        self.assertEqual(q["reason_code"], REASON_UNKNOWN_ENUM)
        self.assertEqual(q["state"], STATE_QUARANTINED)
        self.assertEqual(unresolved_count(self.connection), 1)

    def test_invalid_value_and_missing_timestamp_remain_traceable(self):
        from lakeapp.contract import REASON_INVALID_VALUE
        self.receive_batch([
            {"business_key": "c1", "kind": "upsert", "value": "abc",
             "occurred_at": PART + "T09:00:00"},
            {"business_key": "c2", "kind": "upsert", "value": 1},
        ])
        rows = list_quarantine(self.connection)
        by_key = {r["business_key"]: r for r in rows}
        self.assertEqual(by_key["c1"]["reason_code"], REASON_INVALID_VALUE)
        self.assertIsNone(by_key["c2"]["partition"])  # 无业务时间也保留记录
        self.assertEqual(unresolved_count(self.connection), 2)
        # 补正缺失时间的记录后可正常入仓。
        result = replay_quarantine(
            self.connection, "enrollment", "batch-1", 1,
            corrected={"business_key": "c2", "kind": "upsert", "value": 1,
                       "occurred_at": PART + "T09:00:00"},
        )
        self.assertEqual(result["result"], "stored")

    def test_repeat_batch_is_idempotent_for_quarantine(self):
        records = [good(kind="freeze")]
        self.receive_batch(records)
        self.receive_batch(records)  # 整批重跑
        rows = list_quarantine(self.connection)
        self.assertEqual(len(rows), 1)  # 不重复登记
        self.assertEqual(rows[0]["attempts"], 0)
        self.assertEqual(len(facts(self.connection)), 0)

    def test_stored_record_keeps_terminal_state_on_batch_rerun(self):
        records = [good("b", kind="freeze")]
        self.receive_batch(records)
        replay_quarantine(self.connection, "enrollment", "batch-1", 0,
                          corrected=good("b"))
        self.assertEqual(self.quarantine()["state"], STATE_STORED)
        # 整批重跑：隔离记录保持已入仓终态，事实不重复写入、版本不递增。
        self.receive_batch(records)
        q = self.quarantine()
        self.assertEqual(q["state"], STATE_STORED)
        fact = [f for f in facts(self.connection) if f["business_key"] == "b"][0]
        self.assertEqual(fact["version"], 1)
        self.assertEqual(unresolved_count(self.connection), 0)

    def test_correction_replay_only_reprocesses_affected_row(self):
        self.receive_batch([good("a"), good("b", kind="freeze")])
        self.assertEqual(len(facts(self.connection)), 1)
        result = replay_quarantine(
            self.connection, "enrollment", "batch-1", 1,
            corrected=good("b"),
        )
        self.assertEqual(result["result"], "stored")
        self.assertEqual(result["state"], STATE_STORED)
        self.assertEqual(len(facts(self.connection)), 2)
        q = list_quarantine(self.connection)[0]
        self.assertEqual(q["state"], STATE_STORED)
        self.assertEqual(q["attempts"], 1)
        history = replay_history(self.connection, "enrollment", "batch-1", 1)
        self.assertEqual([h["attempt_no"] for h in history], [1])
        self.assertEqual(history[0]["result"], "stored")
        self.assertEqual(history[0]["input_payload"], good("b"))

    def test_failed_correction_keeps_record_quarantined_and_logs_attempt(self):
        self.receive_batch([good("b", kind="freeze")])
        result = replay_quarantine(
            self.connection, "enrollment", "batch-1", 0,
            corrected={"business_key": "b", "kind": "freeze",
                       "value": 3, "occurred_at": PART + "T09:00:00"},
        )
        self.assertEqual(result["result"], "failed")
        self.assertEqual(result["state"], STATE_QUARANTINED)
        q = self.quarantine()
        self.assertEqual(q["attempts"], 1)
        self.assertEqual(q["last_corrected"]["kind"], "freeze")
        history = replay_history(self.connection, "enrollment", "batch-1", 0)
        self.assertEqual(history[0]["result"], "failed")
        self.assertEqual(len(facts(self.connection)), 0)

    def test_replay_without_correction_uses_latest_payload(self):
        self.receive_batch([good("b", kind="freeze")])
        replay_quarantine(self.connection, "enrollment", "batch-1", 0,
                          corrected=good("b", value=9))
        # 再次无补正重放：走事实表的冲突/版本规则，版本递增而不是重复插入。
        again = replay_quarantine(self.connection, "enrollment", "batch-1", 0)
        self.assertEqual(again["result"], "stored")
        fact = [f for f in facts(self.connection) if f["business_key"] == "b"][0]
        self.assertEqual(fact["version"], 2)
        self.assertEqual(fact["payload"]["value"], 9)

    def test_missing_reference_waits_and_cannot_become_success(self):
        self.receive_batch([good("child", ref="parent-1")])
        q = self.quarantine()
        self.assertEqual(q["state"], STATE_WAITING_REF)
        self.assertEqual(q["reason_code"], REASON_REFERENCE_MISSING)
        self.assertEqual(q["awaited_ref"], "parent-1")
        # 引用未到达时重放仍然是等待，不能变成成功。
        result = replay_quarantine(self.connection, "enrollment", "batch-1", 0)
        self.assertEqual(result["result"], "waiting")
        self.assertEqual(result["state"], STATE_WAITING_REF)
        self.assertEqual(len(facts(self.connection)), 0)

    def test_reference_arrival_unblocks_and_replay_succeeds(self):
        self.receive_batch([good("child", ref="parent-1")])
        # 引用事实到达。
        self.receive_batch([good("parent-1")], batch="batch-2", cursor=2)
        q = self.quarantine()
        self.assertEqual(q["state"], STATE_REPLAYABLE)
        result = replay_quarantine(self.connection, "enrollment", "batch-1", 0)
        self.assertEqual(result["result"], "stored")
        self.assertEqual(len(facts(self.connection)), 2)
        self.assertEqual(unresolved_count(self.connection), 0)

    def test_retry_with_unmet_reference_returns_to_waiting(self):
        # 结构问题隔离的记录补正后带了一个未到达引用：转为等待引用。
        self.receive_batch([good("child", kind="freeze")])
        result = replay_quarantine(
            self.connection, "enrollment", "batch-1", 0,
            corrected=good("child", ref="parent-9"),
        )
        self.assertEqual(result["result"], "waiting")
        self.assertEqual(self.quarantine()["state"], STATE_WAITING_REF)

    def test_trace_shows_raw_reason_and_each_correction(self):
        self.receive_batch([good("b", kind="freeze")])
        replay_quarantine(self.connection, "enrollment", "batch-1", 0,
                          corrected={"business_key": "b", "kind": "freeze",
                                     "value": 5, "occurred_at": PART + "T10:00:00"})
        replay_quarantine(self.connection, "enrollment", "batch-1", 0,
                          corrected=good("b", value=7))
        view = trace_record(self.connection, "enrollment", "batch-1", 0)
        self.assertEqual(view["raw_payload"]["kind"], "freeze")
        self.assertEqual([r["result"] for r in view["replays"]], ["failed", "stored"])
        self.assertEqual(view["current_fact"]["payload"]["value"], 7)
        self.assertEqual(view["current_fact"]["version"], 1)


class PartitionGateTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.connection = open_store(Path(self.tempdir.name) / "data.db")

    def tearDown(self):
        self.connection.close()
        self.tempdir.cleanup()

    def test_domain_state_machine(self):
        self.assertEqual(
            next_state(STATE_QUARANTINED, dependency_ready=False, corrected=True),
            STATE_WAITING_REF)
        self.assertEqual(
            next_state(STATE_WAITING_REF, dependency_ready=True, corrected=False),
            STATE_REPLAYABLE)
        self.assertEqual(
            next_state(STATE_QUARANTINED, dependency_ready=True, corrected=True),
            STATE_REPLAYABLE)
        self.assertEqual(
            next_state(STATE_STORED, dependency_ready=False, corrected=False),
            STATE_STORED)

    def test_gate_policies(self):
        self.assertEqual(partition_gate(0, "strict"), GATE_AVAILABLE)
        self.assertEqual(partition_gate(2, "allow_delay"), GATE_DELAYED)
        self.assertEqual(partition_gate(2, "strict"), GATE_BLOCKED)

    def _seed_one_gap(self):
        receive(self.connection, "enrollment", "batch-1", 1, [good(kind="freeze")])

    def test_publish_blocked_when_gap_unresolved(self):
        self._seed_one_gap()
        quality = partition_quality(self.connection, PART)
        self.assertEqual(quality["gate"], GATE_BLOCKED)
        self.assertEqual(quality["unresolved"], 1)
        result = publish(self.connection, "daily", 1,
                         facts(self.connection, PART), partition=PART)
        self.assertEqual(result["status"], STATUS_BLOCKED)
        with self.assertRaises(KeyError):
            read_manifest(self.connection, "daily", 1)
        version = read_version(self.connection, "daily", 1)
        self.assertEqual(version["status"], STATUS_BLOCKED)
        self.assertEqual(version["manifest"]["quality_gate"], GATE_BLOCKED)

    def test_publish_delayed_under_allow_delay_then_ready_after_replay(self):
        self._seed_one_gap()
        delayed = publish(self.connection, "daily", 1,
                          facts(self.connection, PART),
                          partition=PART, policy="allow_delay")
        self.assertEqual(delayed["status"], STATUS_DELAYED)
        with self.assertRaises(KeyError):
            read_manifest(self.connection, "daily", 1)  # 延迟版本不按就绪读取
        self.assertEqual(read_version(self.connection, "daily", 1)["status"],
                         STATUS_DELAYED)
        replay_quarantine(self.connection, "enrollment", "batch-1", 0,
                          corrected=good())
        ready = publish(self.connection, "daily", 2,
                        facts(self.connection, PART),
                        partition=PART, policy="allow_delay")
        self.assertEqual(ready["status"], STATUS_READY)
        payload = read_manifest(self.connection, "daily", 2)
        self.assertEqual(payload["quality_gate"], GATE_AVAILABLE)
        self.assertEqual(payload["row_count"], 1)

    def test_unrelated_partition_is_available(self):
        self._seed_one_gap()
        other = "2026-11-02"
        quality = partition_quality(self.connection, other)
        self.assertEqual(quality["unresolved"], 0)
        self.assertEqual(quality["gate"], GATE_AVAILABLE)

    def test_duplicate_version_still_conflicts(self):
        publish(self.connection, "daily", 1, [], partition=PART)
        with self.assertRaises(Exception):
            publish(self.connection, "daily", 1, [], partition=PART)


if __name__ == "__main__":
    unittest.main()
