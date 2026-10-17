from __future__ import annotations

import tempfile
import unittest
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


class SnapshotLifecycleTests(unittest.TestCase):
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

    def test_replacement_topics_use_isolated_s3_key_before_cutover(self):
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
        self.assertEqual(
            routed.key, "replacements/replacement-one/db_a/orders"
        )


if __name__ == "__main__":
    unittest.main()
