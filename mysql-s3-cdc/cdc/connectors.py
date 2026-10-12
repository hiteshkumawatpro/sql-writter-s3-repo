"""Generate and reconcile Debezium MySQL connectors."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

from .config import load_databases, load_tables


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
        "schema.history.internal.kafka.topic": f"schemahistory.{db['topic_prefix']}",
        # The default records schema for every table visible to the MySQL user.
        # Limit both schema work and CDC data to the explicitly enabled tables.
        "schema.history.internal.store.only.captured.tables.ddl": "true",
        "include.schema.changes": "false",
        "snapshot.mode": str(db.get("snapshot_mode", "initial")),
        "database.ssl.mode": str(db.get("ssl_mode", "preferred")),
        "decimal.handling.mode": "string",
        "binary.handling.mode": "base64",
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
                if "FAILED" in states and now - last_restart.get(name, 0) >= 10:
                    print(f"WARNING: {name} has a failed task; retrying it.")
                    restart_failed_tasks(name)
                    last_restart[name] = now
            except (OSError, RuntimeError) as exc:
                print(f"WARNING: Could not check/restart {name}: {exc}")
        if pending:
            time.sleep(5)
    return pending


def apply(wait: bool = False) -> None:
    if wait:
        wait_until_ready()
    tables = load_tables()
    databases = load_databases()
    selected = [db for db in databases if eligible_tables(db, tables)]
    desired_names = {connector_name(str(db["name"])) for db in selected}

    # Reconcile previously-created connectors. This removes renamed sources and
    # sources that no longer have any enabled, correctly matched tables.
    registered = request("GET", "/connectors") or []
    stale_names = sorted(
        str(name) for name in registered
        if str(name).startswith(MANAGED_PREFIX) and str(name) not in desired_names
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
        if str(db["name"]) not in selected_names:
            print(
                f"Skipping {connector_name(str(db['name']))}: no enabled tables "
                f"match source {db['name']} and database {db['database']}."
            )

    applied = 0
    applied_names: set[str] = set()
    failed = 0
    for db in selected:
        name = connector_name(str(db["name"]))
        enabled_count = len(eligible_tables(db, tables))
        try:
            # Debezium validates MySQL during this REST request. A failed or
            # firewalled source can take over 30 seconds to return its error.
            put_connector_config(name, build_connector(db, tables))
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

    print(
        f"Connector apply complete: {applied} applied, {failed} failed, "
        f"{removed} stale removed. "
        "Use `python manage.py status` and runtime/logs/connect.log to "
        "inspect asynchronous task failures."
    )


def statuses() -> None:
    names = request("GET", "/connectors")
    managed = [name for name in names if str(name).startswith(MANAGED_PREFIX)]
    if not managed:
        print("No managed connectors are registered.")
    for name in managed:
        status = request("GET", f"/connectors/{name}/status")
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        task_states = ", ".join(str(task.get("state", "UNKNOWN")) for task in status.get("tasks", []))
        print(f"{name}: connector={connector_state}, tasks={task_states or 'none'}")

