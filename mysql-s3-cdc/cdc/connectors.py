"""Generate and reconcile Debezium MySQL connectors."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

from confluent_kafka import Producer

from .config import load_databases, load_tables
from .runtime_state import active_replacements, load_state, update_snapshot, utc_now


CONNECT_URL = os.getenv("CDC_CONNECT_URL", "http://127.0.0.1:8083").rstrip("/")
KAFKA_BOOTSTRAP = os.getenv("CDC_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
MANAGED_PREFIX = "mysql-s3-cdc-"


def connector_name(source: str) -> str:
    safe = "".join(character if character.isalnum() or character in "-_" else "-" for character in source)
    return f"{MANAGED_PREFIX}{safe}"


def eligible_tables(db: dict[str, Any], tables) -> list[Any]:
    """Return enabled tables that belong to this exact source/schema pair."""
    return [
        table for table in tables
        if (
            table.enabled
            and table.source == str(db["name"])
            and table.database == str(db["database"])
        )
    ]


def build_connector(db: dict[str, Any], tables) -> dict[str, str]:
    included = [
        f"{table.database}.{table.table}"
        for table in eligible_tables(db, tables)
    ]
    if not included:
        raise ValueError(
            f"Source {db['name']} has no enabled tables matching database {db['database']}"
        )
    config = {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": str(db["host"]),
        "database.port": str(db.get("port", 3306)),
        "database.user": str(db["username"]),
        "database.password": str(db["password"]),
        "database.server.id": str(db["server_id"]),
        "topic.prefix": str(db["topic_prefix"]),
        "database.include.list": str(db["database"]),
        "table.include.list": ",".join(included),
        "schema.history.internal.kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
        "schema.history.internal.kafka.topic": str(
            db.get("schema_history_topic", f"schemahistory.{db['topic_prefix']}")
        ),
        # Keep DDL for visible tables even before they are selected. This is
        # what lets a later table.include.list update decode the current binlog
        # without damaging the already-running connector.
        "schema.history.internal.store.only.captured.tables.ddl": "false",
        "signal.enabled.channels": "kafka",
        "signal.kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
        "signal.kafka.topic": f"{db['topic_prefix']}-signal",
        "include.schema.changes": "false",
        "snapshot.mode": str(db.get("snapshot_mode", "initial")),
        "database.ssl.mode": str(db.get("ssl_mode", "preferred")),
        "decimal.handling.mode": "string",
        "binary.handling.mode": "base64",
        # Emit DATE/TIME/DATETIME values as ISO text. The sink converts these
        # into native Arrow temporal values before writing Parquet.
        "time.precision.mode": "isostring",
        "tombstones.on.delete": "false",
    }
    if db.get("connection_time_zone"):
        config["database.connectionTimeZone"] = str(db["connection_time_zone"])
    return config


def request(
    method: str,
    path: str,
    payload: Any = None,
    timeout: int = 15,
) -> Any:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        f"{CONNECT_URL}{path}", data=data, method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Kafka Connect {method} {path} failed ({exc.code}): {detail}") from exc


def wait_until_ready(timeout: int = 180) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            request("GET", "/connectors")
            return
        except (OSError, RuntimeError):
            time.sleep(2)
    raise TimeoutError(f"Kafka Connect was not ready after {timeout} seconds")


def restart_failed_tasks(name: str, attempts: int = 4) -> None:
    """Restart failed tasks, tolerating Connect's brief config-rebalance race."""
    path = f"/connectors/{name}/restart?includeTasks=true&onlyFailed=true"
    for attempt in range(1, attempts + 1):
        try:
            request("POST", path, timeout=30)
            return
        except RuntimeError as exc:
            if "failed (409)" not in str(exc) or attempt == attempts:
                raise
            time.sleep(2)


def put_connector_config(name: str, config: dict[str, str], attempts: int = 2) -> None:
    """Apply configuration, retrying one transient MySQL validation reset."""
    for attempt in range(1, attempts + 1):
        try:
            request("PUT", f"/connectors/{name}/config", config, timeout=90)
            return
        except (OSError, RuntimeError) as exc:
            transient = isinstance(exc, OSError) or "Error during connection validation" in str(exc)
            if not transient or attempt == attempts:
                raise
            time.sleep(2)


def recover_failed_tasks(names: set[str], timeout: int = 75) -> set[str]:
    """Wait for tasks to run and periodically retry transient startup failures."""
    pending = set(names)
    unrecoverable: set[str] = set()
    deadline = time.monotonic() + timeout
    last_restart: dict[str, float] = {}
    while pending and time.monotonic() < deadline:
        for name in list(pending):
            try:
                status = request("GET", f"/connectors/{name}/status")
                states = [str(task.get("state", "UNKNOWN")) for task in status.get("tasks", [])]
                if states and all(state == "RUNNING" for state in states):
                    pending.remove(name)
                    continue
                now = time.monotonic()
                traces = " ".join(str(task.get("trace", "")) for task in status.get("tasks", []))
                if "FAILED" in states and fatal_failure(traces):
                    print(f"ERROR: {name} has a non-retryable task failure: {short_failure(traces)}")
                    pending.remove(name)
                    unrecoverable.add(name)
                    continue
                if "FAILED" in states and now - last_restart.get(name, 0) >= 10:
                    print(f"WARNING: {name} has a failed task; retrying it.")
                    restart_failed_tasks(name)
                    last_restart[name] = now
            except (OSError, RuntimeError) as exc:
                print(f"WARNING: Could not check/restart {name}: {exc}")
        if pending:
            time.sleep(5)
    return pending | unrecoverable


FATAL_FAILURE_MARKERS = (
    "schema isn't known",
    "schema isn\u2019t known",
    "no space left on device",
    "requested binlog file is not available",
    "the connector is trying to read binlog",
)


def fatal_failure(trace: str) -> bool:
    lowered = trace.lower()
    return any(marker in lowered for marker in FATAL_FAILURE_MARKERS)


def short_failure(trace: str, limit: int = 500) -> str:
    compact = " ".join(trace.split())
    return compact[:limit] + ("..." if len(compact) > limit else "")


def included_tables(config: dict[str, Any] | None) -> set[str]:
    raw = str((config or {}).get("table.include.list", ""))
    return {item.strip() for item in raw.split(",") if item.strip()}


def primary_runtime_config(db: dict[str, Any], config: dict[str, str]) -> dict[str, str]:
    """Keep a completed recovery history topic stable across later sync/start."""
    recovery = load_state()["recoveries"].get(str(db["name"]))
    if recovery and recovery.get("strategy") == "history" and recovery.get("state") in {
        "recovering", "recovered",
    }:
        config["schema.history.internal.kafka.topic"] = str(
            recovery["recovery_history_topic"]
        )
        config["snapshot.mode"] = (
            "recovery" if recovery["state"] == "recovering" else "no_data"
        )
    return config


def request_blocking_snapshot(db: dict[str, Any], tables: set[str]) -> None:
    """Ask Debezium to snapshot only newly enabled tables, then resume streaming."""
    if not tables:
        return
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    delivery_errors: list[str] = []

    def delivered(error, _message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    payload = {
        "type": "execute-snapshot",
        "data": {
            "type": "blocking",
            "data-collections": sorted(tables),
        },
    }
    producer.produce(
        f"{db['topic_prefix']}-signal",
        key=str(db["topic_prefix"]),
        value=json.dumps(payload, separators=(",", ":")),
        on_delivery=delivered,
    )
    remaining = producer.flush(30)
    if remaining or delivery_errors:
        raise RuntimeError(
            "Kafka did not accept the Debezium snapshot signal: "
            + ("; ".join(delivery_errors) or f"{remaining} message(s) pending")
        )
    for full_name in tables:
        update_snapshot(
            str(db["name"]), full_name.rsplit(".", 1)[-1], state="requested",
            requested_at=utc_now(), snapshot_rows=0,
        )
    print(
        "Requested a blocking snapshot for newly enabled table(s) on "
        f"{db['name']}: {', '.join(sorted(tables))}. Existing connectors and "
        "the S3 sink remain online; this source resumes streaming afterward."
    )


def apply(wait: bool = False, snapshot_new: bool = True, source: str | None = None) -> None:
    if wait:
        wait_until_ready()
    tables = load_tables()
    databases = load_databases()
    if source is not None:
        if not any(str(db["name"]) == source for db in databases):
            raise ValueError(f"Unknown or disabled source: {source}")
        databases = [db for db in databases if str(db["name"]) == source]
    replacements = active_replacements()
    cutover_sources = {
        str(item["source"]) for item in replacements if item.get("state") == "cutover"
    }
    protected_primary_names = {connector_name(item) for item in cutover_sources}
    selected = [
        db for db in databases
        if eligible_tables(db, tables) and str(db["name"]) not in cutover_sources
    ]
    desired_names = {connector_name(str(db["name"])) for db in selected}
    desired_names |= protected_primary_names

    # Reconcile previously-created connectors. This removes renamed sources and
    # sources that no longer have any enabled, correctly matched tables.
    registered = request("GET", "/connectors") or []
    registered_set = {str(name) for name in registered}
    # Only primary connector identities in the selected scope are eligible for
    # removal. Recovery/replacement connectors and unrelated sources are never
    # touched by a source-scoped sync.
    scoped_primary_names = {connector_name(str(db["name"])) for db in databases}
    stale_names = sorted(
        str(name) for name in registered
        if str(name) in scoped_primary_names and str(name) not in desired_names
    )
    removed = 0
    for name in stale_names:
        try:
            request("DELETE", f"/connectors/{name}")
            removed += 1
            print(f"Removed stale connector {name}.")
        except (OSError, RuntimeError) as exc:
            print(f"WARNING: Could not remove stale connector {name}: {exc}")

    selected_names = {str(db["name"]) for db in selected}
    for db in databases:
        if str(db["name"]) in cutover_sources:
            print(
                f"Skipping primary {connector_name(str(db['name']))}: an active "
                "replacement is cut over for this source."
            )
            continue
        if str(db["name"]) not in selected_names:
            print(
                f"Skipping {connector_name(str(db['name']))}: no enabled tables "
                f"match source {db['name']} and database {db['database']}."
            )

    applied = 0
    applied_names: set[str] = set()
    newly_enabled: dict[str, tuple[dict[str, Any], set[str]]] = {}
    failed = 0
    for db in selected:
        name = connector_name(str(db["name"]))
        enabled_count = len(eligible_tables(db, tables))
        try:
            old_config = None
            if name in registered_set:
                old_config = request("GET", f"/connectors/{name}/config")
            desired_config = primary_runtime_config(db, build_connector(db, tables))
            new_tables = included_tables(desired_config) - included_tables(old_config)
            # Debezium validates MySQL during this REST request. A failed or
            # firewalled source can take over 30 seconds to return its error.
            put_connector_config(name, desired_config)
        except (OSError, RuntimeError) as exc:
            failed += 1
            detail = str(exc)
            password = str(db.get("password", ""))
            if password:
                detail = detail.replace(password, "<redacted>")
            print(
                f"WARNING: Could not apply {name}; this source was skipped "
                f"and other connectors will continue: {detail}"
            )
            continue

        applied += 1
        applied_names.add(name)
        # A brand-new connector's initial snapshot already covers all tables.
        if snapshot_new and old_config is not None and new_tables:
            newly_enabled[name] = (db, new_tables)
        print(f"Applied {name} with {enabled_count} enabled table(s).")
        try:
            # Kafka Connect does not necessarily restart a task that was
            # already FAILED when its connector configuration is updated.
            restart_failed_tasks(name)
        except (OSError, RuntimeError) as exc:
            print(f"WARNING: Could not restart failed tasks for {name}: {exc}")

    still_failed = recover_failed_tasks(applied_names) if applied_names else set()
    for name in sorted(still_failed):
        print(
            f"WARNING: {name} did not reach RUNNING task state before the "
            "startup deadline; other sources and the sink will continue."
        )

    for name, (db, new_tables) in newly_enabled.items():
        if name in still_failed:
            print(
                f"ERROR: Did not snapshot newly enabled tables for {name} because "
                "its task is not healthy. Run status and repair schema history first."
            )
            continue
        try:
            request_blocking_snapshot(db, new_tables)
        except (BufferError, RuntimeError) as exc:
            print(f"ERROR: Could not request the new-table snapshot for {name}: {exc}")

    print(
        f"Connector apply complete: {applied} applied, {failed} failed, "
        f"{removed} stale removed. "
        "Use `python manage.py status` and runtime/logs/connect.log to "
        "inspect asynchronous task failures."
    )


def statuses(source: str | None = None) -> bool:
    names = request("GET", "/connectors")
    tables = load_tables()
    databases = load_databases()
    if source is not None:
        if not any(str(db["name"]) == source for db in databases):
            raise ValueError(f"Unknown or disabled source: {source}")
        databases = [db for db in databases if str(db["name"]) == source]
    primary_names = {connector_name(str(db["name"])) for db in databases}
    replacements = active_replacements()
    replacement_names = {
        str(item["connector"]) for item in replacements
        if source is None or item.get("source") == source
    }
    expected_stopped = {
        connector_name(str(item["source"])) for item in replacements
        if item.get("state") == "cutover"
    }
    managed = [name for name in names if str(name) in primary_names | replacement_names]
    expected = {
        connector_name(str(db["name"]))
        for db in databases
        if eligible_tables(db, tables)
    }
    missing = sorted(expected - {str(name) for name in managed})
    if not managed:
        print("No managed connectors are registered.")
        return False
    healthy = not missing
    for name in missing:
        print(f"{name}: MISSING (configured source has enabled tables)")
    for name in managed:
        status = request("GET", f"/connectors/{name}/status")
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        task_states = ", ".join(str(task.get("state", "UNKNOWN")) for task in status.get("tasks", []))
        print(f"{name}: connector={connector_state}, tasks={task_states or 'none'}")
        if name in expected_stopped and connector_state in {"STOPPED", "PAUSED"}:
            print("  expected: original connector preserved and stopped after replacement cutover")
            continue
        if connector_state != "RUNNING" or not status.get("tasks"):
            healthy = False
        for task in status.get("tasks", []):
            if str(task.get("state")) != "RUNNING":
                healthy = False
                trace = str(task.get("trace") or "")
                if trace:
                    print(f"  failure: {short_failure(trace)}")
    return healthy

