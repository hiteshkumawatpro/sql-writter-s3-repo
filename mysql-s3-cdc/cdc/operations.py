"""Safe source-scoped recovery, snapshot, replacement and verification operations."""

from __future__ import annotations

import copy
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import mysql.connector

from .config import load_databases, load_tables
from .connectors import (
    build_connector, connector_name, eligible_tables, fatal_failure,
    put_connector_config, request, request_blocking_snapshot, short_failure,
)
from .runtime_state import (
    RUNTIME, active_replacements, load_state, update_control_entry,
    update_snapshot, utc_now,
)


def source_config(source: str) -> dict[str, Any]:
    matches = [db for db in load_databases() if str(db["name"]) == source]
    if not matches:
        raise ValueError(f"Unknown or disabled source: {source}")
    return matches[0]


def table_config(source: str, table: str):
    db = source_config(source)
    matches = [
        item for item in eligible_tables(db, load_tables()) if item.table == table
    ]
    if not matches:
        raise ValueError(f"{source}.{table} is not an enabled table")
    return db, matches[0]


def connector_backup(source: str) -> tuple[Path, dict[str, Any]]:
    name = connector_name(source)
    backup: dict[str, Any] = {"source": source, "connector": name, "captured_at": utc_now()}
    for key, path in {
        "config": f"/connectors/{name}/config",
        "status": f"/connectors/{name}/status",
        "offsets": f"/connectors/{name}/offsets",
    }.items():
        try:
            backup[key] = request("GET", path)
        except (OSError, RuntimeError) as exc:
            backup[key] = {"unavailable": str(exc)}
    directory = RUNTIME / "recovery" / source
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = directory / f"backup-{stamp}.json"
    path.write_text(json.dumps(backup, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return path, backup


def recovery_plan(source: str) -> bool:
    """Inspect one connector and save a rollback bundle without mutating Connect."""
    db = source_config(source)
    path, backup = connector_backup(source)
    status = backup.get("status") or {}
    traces = " ".join(str(task.get("trace", "")) for task in status.get("tasks", []))
    config = backup.get("config") or {}
    history = config.get("schema.history.internal.kafka.topic", "unknown")
    offsets_available = not isinstance(backup.get("offsets"), dict) or "unavailable" not in backup["offsets"]
    schema_failure = "schema" in traces.lower() and "known" in traces.lower()
    print(f"Recovery plan for {source} ({connector_name(source)})")
    print(f"Rollback bundle: {path}")
    print(f"Current history topic: {history}")
    print(f"Offsets captured: {'yes' if offsets_available else 'no (REST endpoint unavailable)'}")
    print(f"Failure classification: {'missing schema history' if schema_failure else short_failure(traces) or 'no task failure reported'}")
    print("No connector, offset, topic, or S3 state was modified.")
    if schema_failure:
        print(
            "Option A: if a DBA proves relevant DDL compatibility, run guarded "
            "history recovery. Option B: otherwise create a controlled replacement."
        )
    else:
        print("History recovery is not recommended unless the failure is confirmed to be schema-history related.")
    return schema_failure


def begin_history_recovery(
    source: str, ddl_compatible: bool, missing_schema_confirmed: bool = False,
) -> None:
    if not ddl_compatible:
        raise ValueError(
            "History recovery refused: provide --ddl-compatible only after a DBA "
            "confirms current schemas can decode every unprocessed binlog event; "
            "otherwise use recover --strategy replacement"
        )
    db = source_config(source)
    path, backup = connector_backup(source)
    status = backup.get("status") or {}
    traces = " ".join(str(task.get("trace", "")) for task in status.get("tasks", []))
    if (
        (not fatal_failure(traces) or "schema" not in traces.lower())
        and not missing_schema_confirmed
    ):
        raise RuntimeError(
            "Guarded recovery requires a recorded missing-schema task failure. "
            "For an intentionally stopped connector whose REST status lost the "
            "trace, rerun only after log review with --missing-schema-confirmed."
        )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    new_history = f"schemahistory.{db['topic_prefix']}.recovery-{stamp}"
    recovery_db = copy.deepcopy(db)
    recovery_db["schema_history_topic"] = new_history
    recovery_db["snapshot_mode"] = "recovery"
    name = connector_name(source)
    try:
        request("PUT", f"/connectors/{name}/stop")
    except RuntimeError as exc:
        if "failed (409)" not in str(exc):
            raise
    put_connector_config(name, build_connector(recovery_db, load_tables()))
    request("PUT", f"/connectors/{name}/resume")
    update_control_entry(
        "recoveries", source,
        state="recovering", strategy="history", started_at=utc_now(),
        backup=str(path), original_history_topic=(backup.get("config") or {}).get(
            "schema.history.internal.kafka.topic"
        ), recovery_history_topic=new_history,
    )
    print(f"Recovery started for {source} using new history topic {new_history}.")
    print("Original history and offsets were not deleted. Inspect status/connect.log, then run recovery-finalize.")


def finalize_history_recovery(source: str, recovery_complete: bool) -> None:
    if not recovery_complete:
        raise ValueError(
            "Finalization refused: pass --recovery-complete only after connect.log "
            "shows schema recovery completed and the task remains RUNNING"
        )
    state = load_state()
    entry = state["recoveries"].get(source)
    if not entry or entry.get("state") != "recovering":
        raise ValueError(f"No active history recovery exists for {source}")
    name = connector_name(source)
    status = request("GET", f"/connectors/{name}/status")
    tasks = status.get("tasks") or []
    if not tasks or any(str(task.get("state")) != "RUNNING" for task in tasks):
        raise RuntimeError("Recovery cannot be finalized until every connector task is RUNNING")
    config = request("GET", f"/connectors/{name}/config")
    if config.get("schema.history.internal.kafka.topic") != entry["recovery_history_topic"]:
        raise RuntimeError("Connector is not using the recorded recovery history topic")
    config["snapshot.mode"] = "no_data"
    put_connector_config(name, config)
    update_control_entry(
        "recoveries", source, state="recovered", finalized_at=utc_now()
    )
    print(f"History recovery finalized for {source}; offsets and original history remain preserved.")


def snapshot_table(source: str, table: str) -> None:
    db, item = table_config(source, table)
    name = connector_name(source)
    config = request("GET", f"/connectors/{name}/config")
    full_name = f"{item.database}.{item.table}"
    included = {part.strip() for part in str(config.get("table.include.list", "")).split(",")}
    if full_name not in included:
        raise RuntimeError(f"{full_name} is not in the live connector table.include.list; run source-scoped sync first")
    status = request("GET", f"/connectors/{name}/status")
    if any(str(task.get("state")) != "RUNNING" for task in status.get("tasks", [])):
        raise RuntimeError("Snapshot refused because the source connector task is not RUNNING")
    connection = mysql.connector.connect(
        host=db["host"], port=int(db.get("port", 3306)), user=db["username"],
        password=db["password"], database=db["database"],
        ssl_disabled=str(db.get("ssl_mode", "preferred")) == "disabled",
    )
    try:
        cursor = connection.cursor()
        safe_table = table.replace("`", "``")
        cursor.execute(f"SELECT COUNT(*) FROM `{safe_table}`")
        expected_rows = int(cursor.fetchone()[0])
    finally:
        connection.close()
    update_snapshot(
        source, table, state="requested", requested_at=utc_now(),
        snapshot_rows=0, expected_snapshot_rows=expected_rows,
    )
    request_blocking_snapshot(db, {full_name})


def verify_table(source: str, table: str) -> bool:
    db, _item = table_config(source, table)
    state = load_state()
    lifecycle = state["snapshots"].get(f"{source}.{table}", {})
    connection = mysql.connector.connect(
        host=db["host"], port=int(db.get("port", 3306)), user=db["username"],
        password=db["password"], database=db["database"],
        ssl_disabled=str(db.get("ssl_mode", "preferred")) == "disabled",
    )
    try:
        cursor = connection.cursor()
        safe_table = table.replace("`", "``")
        cursor.execute(f"SELECT COUNT(*) FROM `{safe_table}`")
        source_rows = int(cursor.fetchone()[0])
    finally:
        connection.close()
    replacements = [
        item for item in active_replacements() if item.get("source") == source
    ]
    active_connector = (
        str(replacements[-1]["connector"]) if replacements else connector_name(source)
    )
    status = request("GET", f"/connectors/{active_connector}/status")
    running = bool(status.get("tasks")) and all(
        str(task.get("state")) == "RUNNING" for task in status["tasks"]
    )
    observed = int(lifecycle.get("snapshot_rows", 0))
    print(f"{source}.{table}: source_rows_now={source_rows}, snapshot_rows_observed={observed}, connector_running={running}, lifecycle={lifecycle.get('state', 'unknown')}")
    expected = int(lifecycle.get("expected_snapshot_rows", source_rows))
    streaming_observed = lifecycle.get("state") in {"streaming", "complete"}
    verified = running and observed >= expected and streaming_observed
    if verified:
        update_snapshot(source, table, state="complete", verified_at=utc_now(), source_rows=source_rows)
        print("Verification passed. Historical snapshot coverage and a subsequent live CDC event were both observed.")
    else:
        print("Verification is not yet complete; no offsets or state were reset.")
    return verified


def create_replacement(source: str, server_id: int, suffix: str | None = None) -> None:
    db = source_config(source)
    suffix = suffix or datetime.now(timezone.utc).strftime("r%Y%m%d%H%M%S")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", suffix):
        raise ValueError("Replacement suffix may contain only letters, numbers, underscore and hyphen")
    all_ids = {int(item["server_id"]) for item in load_databases()}
    if server_id in all_ids:
        raise ValueError(f"Replacement server ID {server_id} is already configured")
    replacement = copy.deepcopy(db)
    replacement["server_id"] = server_id
    replacement["topic_prefix"] = f"{db['topic_prefix']}-{suffix}"
    replacement["schema_history_topic"] = f"schemahistory.{replacement['topic_prefix']}"
    replacement["snapshot_mode"] = "initial"
    name = f"{connector_name(source)}-replacement-{suffix}"
    if name in (request("GET", "/connectors") or []):
        raise RuntimeError(f"Replacement connector already exists: {name}")
    path, _backup = connector_backup(source)
    put_connector_config(name, build_connector(replacement, load_tables()))
    update_control_entry(
        "replacements", name, state="snapshotting", source=source,
        connector=name, server_id=server_id,
        topic_prefix=replacement["topic_prefix"],
        schema_history_topic=replacement["schema_history_topic"],
        created_at=utc_now(), original_backup=str(path),
    )
    for item in eligible_tables(db, load_tables()):
        update_snapshot(
            source, item.table, state="requested", requested_at=utc_now(),
            snapshot_rows=0, replacement_connector=name,
        )
    print(f"Created replacement {name} with topic prefix {replacement['topic_prefix']}.")
    print(
        "Replacement output uses the same configured table S3 destinations with "
        "topic-specific filenames. The original connector, history, offsets and S3 data were not modified."
    )


def adopt_replacement(source: str, name: str) -> None:
    """Register an existing recovery connector without changing it."""
    db = source_config(source)
    config = request("GET", f"/connectors/{name}/config")
    status = request("GET", f"/connectors/{name}/status")
    if str(config.get("database.include.list")) != str(db["database"]):
        raise RuntimeError(
            f"{name} captures database {config.get('database.include.list')!r}, "
            f"not configured source database {db['database']!r}"
        )
    prefix = str(config.get("topic.prefix") or "")
    if not prefix or prefix == str(db["topic_prefix"]):
        raise RuntimeError("Adopted recovery connector must have a distinct topic.prefix")
    if not status.get("tasks") or any(
        str(task.get("state")) != "RUNNING" for task in status["tasks"]
    ):
        raise RuntimeError(f"Cannot adopt {name}: connector task is not RUNNING")
    update_control_entry(
        "replacements", name, state="cutover", source=source, connector=name,
        server_id=int(config["database.server.id"]), topic_prefix=prefix,
        schema_history_topic=str(config["schema.history.internal.kafka.topic"]),
        adopted_at=utc_now(),
    )
    print(
        f"Adopted existing recovery connector {name} for {source}. Its live "
        "configuration, offsets, topics and history were not modified."
    )


def replacement_cutover(source: str, verified: bool) -> None:
    if not verified:
        raise ValueError(
            "Replacement cutover refused: pass --verified only after reviewing "
            "S3 output, connector health, counts and live CDC continuity"
        )
    state = load_state()
    matches = [
        entry for entry in state["replacements"].values()
        if entry.get("source") == source and entry.get("state") not in {"retired", "rolled_back"}
    ]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one active replacement for {source}; found {len(matches)}")
    replacement = matches[0]
    status = request("GET", f"/connectors/{replacement['connector']}/status")
    if not status.get("tasks") or any(
        str(task.get("state")) != "RUNNING" for task in status["tasks"]
    ):
        raise RuntimeError("Replacement cutover refused until its task is RUNNING")
    request("PUT", f"/connectors/{connector_name(source)}/stop")
    update_control_entry(
        "replacements", str(replacement["connector"]),
        state="cutover", cutover_at=utc_now(),
    )
    print(
        f"Cut over {source} to {replacement['connector']}. The original connector "
        "is stopped, not deleted; its offsets and history remain available for rollback."
    )


def replacement_rollback(source: str) -> None:
    state = load_state()
    matches = [
        entry for entry in state["replacements"].values()
        if entry.get("source") == source and entry.get("state") == "cutover"
    ]
    if len(matches) != 1:
        raise ValueError(f"No cut-over replacement exists for {source}")
    replacement = matches[0]
    request("PUT", f"/connectors/{replacement['connector']}/stop")
    request("PUT", f"/connectors/{connector_name(source)}/resume")
    update_control_entry(
        "replacements", str(replacement["connector"]),
        state="rolled_back", rolled_back_at=utc_now(),
    )
    print(
        f"Rolled back {source}; replacement stopped and original resumed. No connector or topic was deleted."
    )


def _binlog_coordinate(offsets: Any) -> tuple[int, int]:
    """Extract the greatest MySQL binlog file number/position from Connect offsets."""
    found: list[tuple[int, int]] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if value.get("file") is not None and value.get("pos") is not None:
                match = re.search(r"(\d+)$", str(value["file"]))
                found.append((int(match.group(1)) if match else 0, int(value["pos"])))
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(offsets)
    if not found:
        raise RuntimeError("Kafka Connect offsets did not contain a MySQL binlog file/position")
    return max(found)


def handoff_to_primary(source: str, verified: bool) -> None:
    """Retire a caught-up replacement and keep the original connector running."""
    if not verified:
        raise ValueError(
            "Primary handoff refused: pass --verified only after migration, "
            "reconciliation, schema recovery and live CDC validation"
        )
    replacements = [
        item for item in active_replacements() if item.get("source") == source
    ]
    if len(replacements) != 1:
        raise ValueError(f"Expected exactly one active replacement for {source}; found {len(replacements)}")
    replacement = replacements[0]
    primary_name = connector_name(source)
    for name in (primary_name, str(replacement["connector"])):
        status = request("GET", f"/connectors/{name}/status")
        if not status.get("tasks") or any(
            str(task.get("state")) != "RUNNING" for task in status["tasks"]
        ):
            raise RuntimeError(f"Handoff refused because {name} is not RUNNING")
    primary_offset = _binlog_coordinate(
        request("GET", f"/connectors/{primary_name}/offsets")
    )
    replacement_offset = _binlog_coordinate(
        request("GET", f"/connectors/{replacement['connector']}/offsets")
    )
    if primary_offset < replacement_offset:
        raise RuntimeError(
            f"Original connector has not caught up: primary={primary_offset}, "
            f"replacement={replacement_offset}"
        )
    request("PUT", f"/connectors/{replacement['connector']}/stop")
    update_control_entry(
        "replacements", str(replacement["connector"]), state="retired",
        retired_at=utc_now(), primary_offset=list(primary_offset),
        replacement_offset=list(replacement_offset),
    )
    print(
        f"Primary handoff complete for {source}. {primary_name} remains running; "
        f"{replacement['connector']} is stopped but not deleted. Kafka consumer "
        "offsets, topics, schema history and S3 objects were preserved."
    )
