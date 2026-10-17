from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cdc import connectors, operations, runtime_state, sink
from cdc.config import TableConfig


def table(source="source_a", database="db_a", name="orders"):
    return SimpleNamespace(
        source=source, database=database, table=name, enabled=True,
        key=f"{database}/{name}", partition_column=None,
    )


def database(name="source_a"):
    return {
        "name": name, "database": "db_a", "host": "mysql", "port": 3306,
        "username": "cdc", "password": "secret", "server_id": 5401,
        "topic_prefix": name, "snapshot_mode": "initial", "ssl_mode": "disabled",
    }


class ConnectorScopeTests(unittest.TestCase):
    def test_source_sync_never_changes_unrelated_connector(self):
        calls = []

        def request(method, path, payload=None, timeout=15):
            calls.append((method, path))
            if path == "/connectors":
                return ["mysql-s3-cdc-source_a", "mysql-s3-cdc-source_b"]
            if path.endswith("/config") and method == "GET":
                return {"table.include.list": "db_a.orders"}
            return {}

        with (
            patch.object(connectors, "load_databases", return_value=[database("source_a"), database("source_b")]),
            patch.object(connectors, "load_tables", return_value=[table()]),
            patch.object(connectors, "request", side_effect=request),
            patch.object(connectors, "put_connector_config"),
            patch.object(connectors, "restart_failed_tasks"),
            patch.object(connectors, "recover_failed_tasks", return_value=set()),
        ):
            connectors.apply(source="source_a", snapshot_new=False)
        unrelated = [call for call in calls if "source_b" in call[1] and call[0] != "GET"]
        self.assertEqual(unrelated, [])
        self.assertFalse(any(method == "DELETE" for method, _path in calls))

    def test_recovery_requires_explicit_ddl_compatibility(self):
        with self.assertRaisesRegex(ValueError, "History recovery refused"):
            operations.begin_history_recovery("any", ddl_compatible=False)

    def test_replacement_cutover_requires_explicit_verification(self):
        with self.assertRaisesRegex(ValueError, "cutover refused"):
            operations.replacement_cutover("any", verified=False)

    def test_primary_handoff_requires_explicit_verification(self):
        with self.assertRaisesRegex(ValueError, "handoff refused"):
            operations.handoff_to_primary("any", verified=False)

    def test_adopt_replacement_does_not_update_connector(self):
        live = {
            "database.include.list": "db_a", "topic.prefix": "source_a-recovery1",
            "database.server.id": "6401",
            "schema.history.internal.kafka.topic": "schemahistory.recovery1",
        }
        with (
            patch.object(operations, "source_config", return_value=database()),
            patch.object(operations, "request") as request,
            patch.object(operations, "update_control_entry") as update,
        ):
            request.side_effect = [live, {"tasks": [{"state": "RUNNING"}]}]
            operations.adopt_replacement("source_a", "existing-recovery")
        self.assertEqual([call.args[0] for call in request.call_args_list], ["GET", "GET"])
        update.assert_called_once()


class SnapshotLifecycleTests(unittest.TestCase):
    def test_connector_requests_iso_temporal_values(self):
        config = connectors.build_connector(database(), [table()])
        self.assertEqual(config["time.precision.mode"], "isostring")

    def test_datetime_partition_is_floored_to_exact_hour(self):
        configured = table()
        configured.partition_column = "created_at"
        result = sink.partition_hour(
            {"created_at": datetime(2026, 10, 18, 14, 35, 22)}, configured
        )
        self.assertEqual(result, datetime(2026, 10, 18, 14))

    def test_date_only_partition_uses_hour_one(self):
        configured = table()
        configured.partition_column = "business_date"
        result = sink.partition_hour(
            {"business_date": date(2026, 10, 18)}, configured
        )
        self.assertEqual(result, datetime(2026, 10, 18, 1))

    def test_missing_configured_partition_time_uses_event_date_hour_one(self):
        configured = table()
        configured.partition_column = "created_at"
        result = sink.partition_hour(
            {"created_at": None, "_cdc_event_ts_ms": 1792323322000}, configured
        )
        self.assertEqual(result.hour, 1)

    def test_temporal_values_become_native_types(self):
        self.assertEqual(
            sink.normalize_temporal_value(20740, "date"), date(2026, 10, 14)
        )
        value = sink.normalize_temporal_value(1792323322000, "datetime")
        self.assertIsInstance(value, datetime)
        self.assertIsNone(value.tzinfo)
        timestamp = sink.normalize_temporal_value(
            "2026-10-18T14:15:22Z", "timestamp"
        )
        self.assertEqual(timestamp.tzinfo, timezone.utc)

    def test_runtime_snapshot_state_is_durable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            with patch.object(runtime_state, "STATE_PATH", path):
                runtime_state.update_snapshot("source_a", "orders", state="requested")
                runtime_state.record_snapshot_rows("source_a", "orders", 12, False)
                runtime_state.record_snapshot_rows("source_a", "orders", 0, True)
                entry = runtime_state.load_state()["snapshots"]["source_a.orders"]
            self.assertEqual(entry["snapshot_rows"], 12)
            self.assertEqual(entry["state"], "streaming")

    def test_event_id_is_stable_for_kafka_coordinate(self):
        message = SimpleNamespace(
            topic=lambda: "prefix.db.orders", partition=lambda: 0,
            offset=lambda: 42,
        )
        configured = SimpleNamespace(
            source="source_a", database="db_a", table="orders",
            accepts=lambda operation: True,
        )
        envelope = {"op": "c", "after": {"id": 1}, "source": {"file": "binlog.1", "pos": 2}}
        first = sink.event_record(envelope, message, configured)
        second = sink.event_record(envelope, message, configured)
        self.assertEqual(first["_cdc_event_id"], second["_cdc_event_id"])
        self.assertEqual(len(first["_cdc_event_id"]), 64)

    def test_live_event_id_survives_connector_topic_change(self):
        first_message = SimpleNamespace(
            topic=lambda: "original.db.orders", partition=lambda: 0,
            offset=lambda: 42,
        )
        second_message = SimpleNamespace(
            topic=lambda: "recovery.db.orders", partition=lambda: 0,
            offset=lambda: 999,
        )
        configured = SimpleNamespace(
            source="source_a", database="db_a", table="orders",
            accepts=lambda operation: True,
        )
        envelope = {
            "op": "u", "after": {"id": 1},
            "source": {"file": "binlog.000007", "pos": 1234, "row": 2},
        }
        first = sink.event_record(envelope, first_message, configured)
        second = sink.event_record(envelope, second_message, configured)
        self.assertEqual(first["_cdc_event_id"], second["_cdc_event_id"])

    def test_binlog_offset_comparison_uses_file_then_position(self):
        offsets = {"offsets": [{"offset": {"file": "binlog.000009", "pos": 88}}]}
        self.assertEqual(operations._binlog_coordinate(offsets), (9, 88))

    def test_explicit_snapshot_works_when_table_is_already_included(self):
        configured_table = table()
        connection = SimpleNamespace(
            cursor=lambda: SimpleNamespace(
                execute=lambda _query: None, fetchone=lambda: (10,)
            ),
            close=lambda: None,
        )
        with (
            patch.object(operations, "table_config", return_value=(database(), configured_table)),
            patch.object(operations, "request") as request,
            patch.object(operations, "update_snapshot"),
            patch.object(operations, "request_blocking_snapshot") as snapshot,
            patch.object(operations.mysql.connector, "connect", return_value=connection),
        ):
            request.side_effect = [
                {"table.include.list": "db_a.orders"},
                {"tasks": [{"state": "RUNNING"}]},
            ]
            operations.snapshot_table("source_a", "orders")
        snapshot.assert_called_once()

    def test_schema_union_keeps_added_columns(self):
        rows = sink.normalized_rows([{"id": 1}, {"id": 2, "new_column": "x"}])
        self.assertEqual(rows[0], {"id": 1, "new_column": None})
        self.assertEqual(rows[1]["new_column"], "x")

    def test_completed_history_recovery_survives_later_sync(self):
        config = {"snapshot.mode": "initial", "schema.history.internal.kafka.topic": "old"}
        state = {
            "recoveries": {"source_a": {
                "strategy": "history", "state": "recovered",
                "recovery_history_topic": "new-history",
            }}
        }
        with patch.object(connectors, "load_state", return_value=state):
            result = connectors.primary_runtime_config(database(), config)
        self.assertEqual(result["snapshot.mode"], "no_data")
        self.assertEqual(result["schema.history.internal.kafka.topic"], "new-history")

    def test_replacement_topics_keep_configured_s3_key(self):
        configured = TableConfig(
            1, "source_a", "db_a", "orders", "db_a/orders",
            True, True, True, True, None,
        )
        replacement = {
            "source": "source_a", "connector": "replacement-one",
            "topic_prefix": "source_a-recovery", "state": "snapshotting",
        }
        with (
            patch.object(sink, "load_databases", return_value=[database()]),
            patch.object(sink, "load_tables", return_value=[configured]),
            patch.object(sink, "active_replacements", return_value=[replacement]),
        ):
            mapping = sink.topic_map()
        routed = mapping["source_a-recovery.db_a.orders"]
        self.assertEqual(routed.key, "db_a/orders")

    def test_event_ledger_suppresses_an_archived_event(self):
        configured = TableConfig(
            1, "source_a", "db_a", "orders", "db_a/orders",
            True, True, True, True, None,
        )
        rows = [{"_cdc_event_id": "event-one"}]
        with tempfile.TemporaryDirectory() as directory, patch.object(
            sink, "RUNTIME", Path(directory)
        ):
            ledger = sink.EventLedger()
            self.assertEqual(ledger.unseen(configured, rows), rows)
            ledger.record(configured, rows, "object.parquet")
            self.assertEqual(ledger.unseen(configured, rows), [])
            ledger.close()


if __name__ == "__main__":
    unittest.main()
