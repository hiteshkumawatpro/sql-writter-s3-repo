"""Generate and reconcile Debezium MySQL connectors."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

from .config import load_databases, load_tables


CONNECT_URL = os.getenv("CDC_CONNECT_URL", "http://localhost:8083").rstrip("/")
KAFKA_BOOTSTRAP = os.getenv("CDC_KAFKA_BOOTSTRAP", "127.0.0.1:9092")


def connector_name(source: str) -> str:
    safe = "".join(character if character.isalnum() or character in "-_" else "-" for character in source)
    return f"mysql-s3-cdc-{safe}"


def build_connector(db: dict[str, Any], tables) -> dict[str, str]:
    included = [
        f"{table.database}.{table.table}" for table in tables
        if table.enabled and table.source == str(db["name"])
    ]
    # The sentinel matches nothing and safely stops capture when all tables are disabled.
    table_include = ",".join(included) or r"__cdc_disabled__\.__cdc_disabled__"
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
        "table.include.list": table_include,
        "schema.history.internal.kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
        "schema.history.internal.kafka.topic": f"schemahistory.{db['topic_prefix']}",
        "include.schema.changes": "false",
        "snapshot.mode": str(db.get("snapshot_mode", "initial")),
        "decimal.handling.mode": "string",
        "binary.handling.mode": "base64",
        "tombstones.on.delete": "false",
    }
    if db.get("connection_time_zone"):
        config["database.connectionTimeZone"] = str(db["connection_time_zone"])
    return config


def request(method: str, path: str, payload: Any = None) -> Any:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        f"{CONNECT_URL}{path}", data=data, method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
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


def apply(wait: bool = False) -> None:
    if wait:
        wait_until_ready()
    tables = load_tables()
    for db in load_databases():
        name = connector_name(str(db["name"]))
        request("PUT", f"/connectors/{name}/config", build_connector(db, tables))
        enabled_count = sum(table.enabled and table.source == str(db["name"]) for table in tables)
        print(f"Applied {name} with {enabled_count} enabled table(s).")


def statuses() -> None:
    names = request("GET", "/connectors")
    managed = [name for name in names if str(name).startswith("mysql-s3-cdc-")]
    if not managed:
        print("No managed connectors are registered.")
    for name in managed:
        status = request("GET", f"/connectors/{name}/status")
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        task_states = ", ".join(str(task.get("state", "UNKNOWN")) for task in status.get("tasks", []))
        print(f"{name}: connector={connector_state}, tasks={task_states or 'none'}")

