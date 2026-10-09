"""Configuration loading and validation shared by all services."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
DATABASES_PATH = Path(os.getenv("CDC_DATABASES_CONFIG", "config/databases.yml"))
TABLES_PATH = Path(os.getenv("CDC_TABLES_CONFIG", "config/tables.yml"))
S3_PATH = Path(os.getenv("CDC_S3_CONFIG", "config/s3.yml"))


@dataclass(frozen=True)
class TableConfig:
    order: int
    source: str
    database: str
    table: str
    key: str
    enabled: bool
    insert: bool
    update: bool
    delete: bool
    partition_column: str | None = None

    def accepts(self, operation: str) -> bool:
        if operation in {"c", "r"}:
            return self.insert
        if operation == "u":
            return self.update
        if operation == "d":
            return self.delete
        return False


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in os.environ:
                raise ValueError(f"Environment variable {name} is not set")
            return os.environ[name]
        return ENV_PATTERN.sub(replace, value)
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    return value


def load_yaml(path: Path, expand_environment: bool = True) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle) or {}
    if not isinstance(document, dict):
        raise ValueError(f"{path} must contain a YAML object")
    return _expand(document) if expand_environment else document


def load_databases(expand_environment: bool = True) -> list[dict[str, Any]]:
    databases = load_yaml(DATABASES_PATH, expand_environment).get("databases")
    if not isinstance(databases, list) or not databases:
        raise ValueError("databases.yml must contain a non-empty 'databases' list")
    enabled = []
    names: set[str] = set()
    prefixes: set[str] = set()
    server_ids: set[int] = set()
    for index, db in enumerate(databases, 1):
        if not isinstance(db, dict):
            raise ValueError(f"Database entry #{index} must be an object")
        if not db.get("enabled", True):
            continue
        required = ("name", "host", "username", "password", "database", "server_id", "topic_prefix")
        missing = [field for field in required if db.get(field) in (None, "")]
        if missing:
            raise ValueError(f"Database entry #{index} is missing: {', '.join(missing)}")
        if str(db.get("driver", "mysql")).lower() != "mysql":
            raise ValueError("Only the mysql driver is currently supported")
        name = str(db["name"])
        prefix = str(db["topic_prefix"])
        server_id = int(db["server_id"])
        if name in names or prefix in prefixes or server_id in server_ids:
            raise ValueError("Enabled database name, topic_prefix, and server_id values must be unique")
        names.add(name)
        prefixes.add(prefix)
        server_ids.add(server_id)
        enabled.append(db)
    return enabled


def _strict_bool(item: dict[str, Any], field: str, identity: str) -> bool:
    value = item.get(field, False)
    if not isinstance(value, bool):
        raise ValueError(f"{identity}.{field} must be true or false")
    return value


def load_tables() -> list[TableConfig]:
    entries = load_yaml(TABLES_PATH, expand_environment=False).get("tables")
    if not isinstance(entries, list):
        raise ValueError("tables.yml must contain a 'tables' list")
    result = []
    identities: set[tuple[str, str, str]] = set()
    keys: set[str] = set()
    for index, item in enumerate(entries, 1):
        if not isinstance(item, dict):
            raise ValueError(f"Table entry #{index} must be an object")
        required = ("order", "source", "database", "table", "key")
        missing = [field for field in required if item.get(field) in (None, "")]
        if missing:
            raise ValueError(f"Table entry #{index} is missing: {', '.join(missing)}")
        identity = (str(item["source"]), str(item["database"]), str(item["table"]))
        identity_text = ".".join(identity)
        if identity in identities:
            raise ValueError(f"Duplicate table entry: {identity_text}")
        key = str(item["key"]).strip("/")
        if key in keys:
            raise ValueError(f"Duplicate S3 key: {key}")
        identities.add(identity)
        keys.add(key)
        result.append(TableConfig(
            order=int(item["order"]), source=identity[0], database=identity[1],
            table=identity[2], key=key,
            enabled=_strict_bool(item, "enabled", identity_text),
            insert=_strict_bool(item, "insert", identity_text),
            update=_strict_bool(item, "update", identity_text),
            delete=_strict_bool(item, "delete", identity_text),
            partition_column=item.get("partition_column"),
        ))
    return sorted(result, key=lambda table: table.order)


def load_s3() -> dict[str, Any]:
    document = load_yaml(S3_PATH)
    s3 = document.get("s3")
    sink = document.get("sink", {})
    if not isinstance(s3, dict) or not s3.get("bucket"):
        raise ValueError("s3.yml must contain s3.bucket")
    if not isinstance(sink, dict):
        raise ValueError("s3.yml sink must be an object")
    if int(sink.get("batch_size", 1000)) <= 0 or float(sink.get("flush_seconds", 30)) <= 0:
        raise ValueError("sink.batch_size and sink.flush_seconds must be positive")
    document["sink"] = sink
    return document


def validate_all() -> None:
    databases = load_databases()
    tables = load_tables()
    load_s3()
    sources = {str(db["name"]): str(db["database"]) for db in databases}
    for table in tables:
        if table.source not in sources:
            raise ValueError(f"Table {table.source}.{table.table} refers to an unknown/disabled source")
        if table.database != sources[table.source]:
            raise ValueError(f"Table {table.source}.{table.table} has a database mismatch")

