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


def discover() -> int:
    discovered: list[dict[str, Any]] = []
    for db in load_databases():
        print(f"Inspecting {db['name']}: {db['host']}:{db.get('port', 3306)}/{db['database']}")
        connection = mysql.connector.connect(
            host=db["host"], port=int(db.get("port", 3306)), user=db["username"],
            password=db["password"], database=db["database"], connection_timeout=15,
        )
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME",
                (db["database"],),
            )
            for (table_name,) in cursor.fetchall():
                discovered.append({"source": str(db["name"]), "database": str(db["database"]), "table": str(table_name)})
            cursor.close()
        finally:
            connection.close()

    with file_lock(TABLES_PATH):
        document = load_yaml(TABLES_PATH, expand_environment=False)
        tables = document.get("tables")
        if not isinstance(tables, list):
            raise ValueError("tables.yml must contain a 'tables' list")
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
            tables.append({
                "order": next_order, **item,
                "key": f"{item['source']}/{item['database']}/{item['table']}",
                "partition_column": None,
                "enabled": False, "insert": True, "update": True, "delete": True,
            })
            next_order += 1
            added += 1
        if added:
            _write_atomic(TABLES_PATH, document)
    print(f"Discovery complete: appended {added} new table(s); existing entries were preserved.")
    return added

