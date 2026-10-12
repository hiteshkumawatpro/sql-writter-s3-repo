"""Append-only MySQL table discovery."""

from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import mysql.connector
import yaml

from .config import TABLES_PATH, load_databases, load_yaml


REQUIRED_CDC_PRIVILEGES = (
    "SELECT",
    "RELOAD",
    "SHOW DATABASES",
    "REPLICATION CLIENT",
)


def cdc_readiness_issues(connection) -> list[str]:
    """Return actionable reasons why a MySQL source is not CDC-ready."""
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, "
            "@@GLOBAL.binlog_row_image"
        )
        log_bin, binlog_format, binlog_row_image = cursor.fetchone()

        cursor.execute("SHOW GRANTS FOR CURRENT_USER")
        grants = " ".join(str(row[0]) for row in cursor.fetchall()).upper()
    finally:
        cursor.close()

    issues = []
    if str(log_bin).upper() not in {"1", "ON"}:
        issues.append("MySQL binary logging is disabled")
    if str(binlog_format).upper() != "ROW":
        issues.append(f"binlog_format is {binlog_format!s}, expected ROW")
    if str(binlog_row_image).upper() != "FULL":
        issues.append(
            f"binlog_row_image is {binlog_row_image!s}, expected FULL"
        )

    has_global_all = "ALL PRIVILEGES ON *.*" in grants
    missing = [
        privilege
        for privilege in REQUIRED_CDC_PRIVILEGES
        if not has_global_all and privilege not in grants
    ]
    if (
        not has_global_all
        and "REPLICATION SLAVE" not in grants
        and "REPLICATION REPLICA" not in grants
    ):
        missing.append("REPLICATION SLAVE/REPLICA")
    if missing:
        issues.append("CDC user is missing grants: " + ", ".join(missing))

    return issues


@contextmanager
def file_lock(path: Path):
    lock_path = Path(f"{path}.lock")
    with lock_path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _write_atomic(path: Path, document: dict[str, Any]) -> None:
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            yaml.safe_dump(document, handle, sort_keys=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def discover(enable_new: bool = False) -> int:
    discovered: list[dict[str, Any]] = []
    skipped = 0
    for db in load_databases():
        print(f"Inspecting {db['name']}: {db['host']}:{db.get('port', 3306)}/{db['database']}")
        try:
            connection = mysql.connector.connect(
                host=db["host"],
                port=int(db.get("port", 3306)),
                user=db["username"],
                password=db["password"],
                database=db["database"],
                connection_timeout=15,
            )
            try:
                issues = cdc_readiness_issues(connection)
                if issues:
                    skipped += 1
                    print(
                        f"WARNING: Skipping {db['name']}; CDC prerequisites "
                        f"are not met: {'; '.join(issues)}"
                    )
                    continue

                cursor = connection.cursor()
                try:
                    cursor.execute(
                        "SELECT TABLE_NAME FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' "
                        "ORDER BY TABLE_NAME",
                        (db["database"],),
                    )
                    for (table_name,) in cursor.fetchall():
                        discovered.append({
                            "source": str(db["name"]),
                            "database": str(db["database"]),
                            "table": str(table_name),
                        })
                finally:
                    cursor.close()
            finally:
                connection.close()
        except mysql.connector.Error as exc:
            skipped += 1
            detail = getattr(exc, "msg", None) or str(exc)
            print(
                f"WARNING: Skipping {db['name']}; MySQL CDC readiness "
                f"check failed: {detail}"
            )

    with file_lock(TABLES_PATH):
        document = (
            load_yaml(TABLES_PATH, expand_environment=False)
            if TABLES_PATH.exists()
            else {}
        )
        tables = document.get("tables")
        initialized = tables is None
        if initialized:
            tables = []
            document["tables"] = tables
        elif not isinstance(tables, list):
            raise ValueError("tables.yml must contain a 'tables' list")

        discovered_sources: dict[tuple[str, str], set[str]] = {}
        for item in discovered:
            key = (item["database"], item["table"])
            discovered_sources.setdefault(key, set()).add(item["source"])

        reconciled = 0
        for item in tables:
            if not isinstance(item, dict):
                continue
            key = (str(item.get("database")), str(item.get("table")))
            candidates = discovered_sources.get(key, set())
            current_source = str(item.get("source"))
            if len(candidates) == 1 and current_source not in candidates:
                new_source = next(iter(candidates))
                print(
                    f"WARNING: Updating table source for "
                    f"{key[0]}.{key[1]} from {current_source} to "
                    f"{new_source}; table settings are preserved."
                )
                item["source"] = new_source
                reconciled += 1

        existing = {
            (str(item.get("source")), str(item.get("database")), str(item.get("table")))
            for item in tables if isinstance(item, dict)
        }
        next_order = max((int(item.get("order", 0)) for item in tables if isinstance(item, dict)), default=0) + 1
        added = 0
        for item in discovered:
            identity = (item["source"], item["database"], item["table"])
            if identity in existing:
                continue
            new_table = {
                "order": next_order, **item,
                "key": f"{item['source']}/{item['database']}/{item['table']}",
                "partition_column": None,
                "enabled": enable_new,
                "insert": True,
                "update": True,
                "delete": True,
            }
            if enable_new:
                new_table.update({"last_lsn": None, "store_key": None})
            tables.append(new_table)
            next_order += 1
            added += 1
        if added or initialized or reconciled:
            _write_atomic(TABLES_PATH, document)
    state = "enabled" if enable_new else "disabled"
    print(
        f"Discovery complete: appended {added} new {state} table(s); "
        f"reconciled {reconciled} source name(s); skipped {skipped} "
        "non-ready source(s); existing table settings were preserved."
    )
    return added

