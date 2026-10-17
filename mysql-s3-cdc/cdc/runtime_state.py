"""Durable, Git-ignored operational state shared by manager processes."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .discovery import file_lock


RUNTIME = Path(os.getenv("CDC_RUNTIME_DIR", "runtime"))
STATE_PATH = RUNTIME / "control-state.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"version": 1, "snapshots": {}, "recoveries": {}, "replacements": {}}
    with STATE_PATH.open(encoding="utf-8") as handle:
        state = json.load(handle)
    for key in ("snapshots", "recoveries", "replacements"):
        state.setdefault(key, {})
    return state


def _save_unlocked(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=STATE_PATH.name + ".", suffix=".tmp", dir=STATE_PATH.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, STATE_PATH)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(STATE_PATH):
        _save_unlocked(state)


def update_snapshot(source: str, table: str, **changes: Any) -> dict[str, Any]:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(STATE_PATH):
        state = load_state()
        key = f"{source}.{table}"
        entry = state["snapshots"].setdefault(key, {
            "source": source, "table": table, "snapshot_rows": 0,
        })
        entry.update(changes)
        entry["updated_at"] = utc_now()
        _save_unlocked(state)
        return entry

def record_snapshot_rows(source: str, table: str, count: int, streaming: bool) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(STATE_PATH):
        state = load_state()
        key = f"{source}.{table}"
        entry = state["snapshots"].get(key)
        if not entry:
            return
        if count:
            entry["snapshot_rows"] = int(entry.get("snapshot_rows", 0)) + count
            entry["state"] = "snapshotting"
            entry.setdefault("snapshot_started_at", utc_now())
        if streaming and entry.get("state") in {"requested", "snapshotting"}:
            entry["state"] = "streaming"
            entry["streaming_observed_at"] = utc_now()
        entry["updated_at"] = utc_now()
        _save_unlocked(state)


def active_replacements() -> list[dict[str, Any]]:
    return [
        item for item in load_state()["replacements"].values()
        if item.get("state") not in {"retired", "rolled_back"}
    ]


def update_control_entry(section: str, key: str, **changes: Any) -> dict[str, Any]:
    if section not in {"recoveries", "replacements"}:
        raise ValueError(f"Unsupported control-state section: {section}")
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(STATE_PATH):
        state = load_state()
        entry = state[section].setdefault(key, {})
        entry.update(changes)
        entry["updated_at"] = utc_now()
        _save_unlocked(state)
        return entry

