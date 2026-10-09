"""Incrementally export tables from multiple MySQL databases to Parquet files on S3."""

import argparse
from contextlib import contextmanager
import logging
import os
import re
import signal
import tempfile
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import mysql.connector
import yaml


LOG = logging.getLogger("mysql-s3-exporter")
ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def load_yaml(path: str, expand_environment: bool = True) -> dict[str, Any]:
    """Load a YAML file and optionally expand ${ENVIRONMENT_VARIABLES}."""
    with open(path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    def expand(value):
        if isinstance(value, str):

            def replace(match):
                name = match.group(1)

                if name not in os.environ:
                    raise ValueError(
                        f"Environment variable {name} is not set"
                    )

                return os.environ[name]

            return ENV_PATTERN.sub(replace, value)

        if isinstance(value, list):
            return [expand(item) for item in value]

        if isinstance(value, dict):
            return {
                key: expand(item)
                for key, item in value.items()
            }

        return value

    return expand(data) if expand_environment else data


def get_database_configs(
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Return all configured databases.

    Preferred format:

    databases:
      - name: database_one
        ...
      - name: database_two
        ...

    The old single-database format is also supported:

    database:
      driver: mysql
      ...
    """
    if "databases" in config:
        databases = config["databases"]

    elif "database" in config:
        databases = config["database"]

        # Support the old single-database dictionary format.
        if isinstance(databases, dict):
            databases = [databases]

        # Also tolerate a list accidentally placed under "database".
        elif isinstance(databases, list):
            LOG.warning(
                "Using a list under 'database'. "
                "Rename this YAML key to 'databases'."
            )

    else:
        raise ValueError(
            "db.yml must contain either 'databases' or 'database'."
        )

    if not isinstance(databases, list):
        raise ValueError(
            "'databases' must contain a YAML list."
        )

    if not databases:
        raise ValueError(
            "No database configurations were provided."
        )

    for index, database_config in enumerate(databases, start=1):
        if not isinstance(database_config, dict):
            raise ValueError(
                f"Database entry #{index} must be a YAML object, "
                f"not {type(database_config).__name__}."
            )

    return databases


def validate_database_config(
    db: dict[str, Any],
    index: int,
) -> None:
    """Validate the required fields for one database connection."""
    required_fields = [
        "host",
        "username",
        "password",
        "database",
    ]

    missing = [
        field
        for field in required_fields
        if not db.get(field)
    ]

    if missing:
        raise ValueError(
            f"Database entry #{index} is missing: "
            f"{', '.join(missing)}"
        )

    driver = str(db.get("driver", "mysql")).lower()

    if driver != "mysql":
        raise ValueError(
            f"Database entry #{index} uses unsupported "
            f"driver {driver!r}. Only MySQL is supported."
        )


def get_source_name(db: dict[str, Any]) -> str:
    """
    Return the name used by table_config.yml to identify a database.

    The explicit 'name' is preferred. If it is missing, the MySQL database
    name is used.
    """
    source_name = db.get("name") or db.get("database")

    if not source_name:
        raise ValueError(
            "Every database must have either 'name' or 'database'."
        )

    return str(source_name)


def build_database_registry(
    database_configs: list[dict[str, Any]],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, list[dict[str, Any]]],
    list[dict[str, Any]],
]:
    """
    Build lookup registries for resolving each table to its database.

    Returns:
        by_source:
            Database configurations indexed by their configured name.

        by_schema:
            Database configurations indexed by their MySQL database/schema.

        enabled_databases:
            All enabled database configurations.
    """
    by_source: dict[str, dict[str, Any]] = {}
    by_schema: dict[str, list[dict[str, Any]]] = defaultdict(list)
    enabled_databases: list[dict[str, Any]] = []

    for index, db in enumerate(database_configs, start=1):
        if not db.get("enabled", True):
            LOG.info(
                "Skipping disabled database configuration #%d",
                index,
            )
            continue

        validate_database_config(db, index)

        source_name = get_source_name(db)
        schema_name = str(db["database"])

        if source_name in by_source:
            raise ValueError(
                f"Duplicate database source name {source_name!r}. "
                "Every database entry must have a unique name."
            )

        by_source[source_name] = db
        by_schema[schema_name].append(db)
        enabled_databases.append(db)

    return by_source, by_schema, enabled_databases


def resolve_table_database(
    table: dict[str, Any],
    by_source: dict[str, dict[str, Any]],
    by_schema: dict[str, list[dict[str, Any]]],
    enabled_databases: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Resolve the appropriate database configuration for a table.

    Resolution order:

    1. table.source
    2. table.database
    3. The only configured database, for backward compatibility
    """
    source = table.get("source")
    declared_database = table.get("database")

    if source:
        source = str(source)

        if source not in by_source:
            # The source may be the schema name rather than the configured name.
            schema_matches = by_schema.get(source, [])

            if len(schema_matches) == 1:
                selected = schema_matches[0]
            else:
                configured_sources = ", ".join(sorted(by_source))

                raise ValueError(
                    f"Table {table.get('table')!r} refers to unknown "
                    f"source {source!r}. Configured sources: "
                    f"{configured_sources}"
                )
        else:
            selected = by_source[source]

        if (
            declared_database
            and str(declared_database) != str(selected["database"])
        ):
            raise ValueError(
                f"Table {table.get('table')!r} declares "
                f"source={source!r} but database="
                f"{declared_database!r}. The selected source points to "
                f"database={selected['database']!r}."
            )

        return selected

    if declared_database:
        schema_matches = by_schema.get(
            str(declared_database),
            [],
        )

        if len(schema_matches) == 1:
            return schema_matches[0]

        if len(schema_matches) > 1:
            raise ValueError(
                f"Multiple configured connections use database "
                f"{declared_database!r}. Add a 'source' field to "
                f"table {table.get('table')!r}."
            )

        raise ValueError(
            f"No enabled connection was found for database "
            f"{declared_database!r}, required by table "
            f"{table.get('table')!r}."
        )

    if len(enabled_databases) == 1:
        return enabled_databases[0]

    raise ValueError(
        f"Table {table.get('table')!r} does not contain a "
        "'source' or 'database' field. These fields are required "
        "when multiple databases are configured."
    )


def connect(db: dict[str, Any]):
    """Connect to one configured MySQL database."""
    if str(db.get("driver", "mysql")).lower() != "mysql":
        raise ValueError("Only the mysql driver is supported")

    connection = mysql.connector.connect(
        host=db["host"],
        port=int(db.get("port", 3306)),
        user=db["username"],
        password=db["password"],
        database=db["database"],
        connection_timeout=int(
            db.get("connection_timeout", 15)
        ),
        autocommit=True,
    )

    time_zone = db.get("time_zone")

    if time_zone:
        cursor = connection.cursor()

        try:
            cursor.execute(
                "SET SESSION time_zone = %s",
                (str(time_zone),),
            )
        finally:
            cursor.close()

    return connection


def quote(name: str) -> str:
    """Safely quote a MySQL identifier."""
    if not name or "\x00" in name:
        raise ValueError("Invalid SQL identifier")

    return "`" + name.replace("`", "``") + "`"


def save_yaml(path: str, data: dict[str, Any]) -> None:
    """Atomically replace YAML so interruption cannot leave a partial file."""
    path_object = Path(path)
    path_object.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary = tempfile.mkstemp(
        prefix=f".{path_object.name}.",
        dir=path_object.parent,
        text=True,
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
        ) as handle:
            yaml.safe_dump(
                data,
                handle,
                sort_keys=False,
            )
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(
            temporary,
            path_object,
        )

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def table_config_lock(path: str | Path):
    """Coordinate table-config updates between discovery and the exporter."""
    lock_path = Path(f"{path}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)

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


def table_identity(table: dict[str, Any]) -> tuple[str, str, str]:
    """Return the stable identity of a configured source table."""
    return (
        str(table.get("source") or ""),
        str(table.get("database") or ""),
        str(table.get("table") or ""),
    )


def save_table_progress(
    table_path: str,
    table: dict[str, Any],
    last_read: str,
    new_offset: Any = None,
    advance_offset: bool = False,
) -> None:
    """
    Merge one table's progress into the latest YAML document.

    Reloading before the atomic write preserves tables and settings appended or
    edited after the current export cycle started.
    """
    with table_config_lock(table_path):
        latest_document = load_yaml(
            table_path,
            expand_environment=False,
        )
        latest_tables = latest_document.get("tables")

        if not isinstance(latest_tables, list):
            raise ValueError(
                "table_config.yml does not contain a 'tables' list."
            )

        identity = table_identity(table)
        matches = [
            candidate
            for candidate in latest_tables
            if isinstance(candidate, dict)
            and table_identity(candidate) == identity
        ]

        if len(matches) != 1:
            raise ValueError(
                f"Expected exactly one current configuration entry for "
                f"source table {identity!r}; found {len(matches)}."
            )

        current = matches[0]

        if advance_offset:
            expected_offset = yaml_value(table.get("offset_value"))

            if yaml_value(current.get("offset_value")) != expected_offset:
                raise RuntimeError(
                    f"Offset for source table {identity!r} changed while "
                    "its batch was exporting; preserving the externally "
                    "edited value."
                )

            current["offset_value"] = yaml_value(new_offset)

        current["last_read"] = last_read
        save_yaml(table_path, latest_document)

    table["last_read"] = last_read

    if advance_offset:
        table["offset_value"] = yaml_value(new_offset)


def fetch_batch(
    connection,
    table: dict[str, Any],
    offset,
    batch_size: int,
) -> list[dict[str, Any]]:
    """Fetch the next ordered batch from one MySQL table."""
    offset_column_name = table.get("offset_column")

    if not offset_column_name:
        raise ValueError(
            f"Table {table.get('table')!r} has no offset_column."
        )

    offset_column = quote(offset_column_name)

    where = ""
    params = []

    if offset is not None:
        where = f" WHERE {offset_column} > %s"
        params.append(offset)

    sql = (
        f"SELECT * FROM {quote(table['table'])}"
        f"{where} "
        f"ORDER BY {offset_column} ASC "
        f"LIMIT %s"
    )

    params.append(batch_size)

    cursor = connection.cursor(dictionary=True)

    try:
        cursor.execute(
            sql,
            params,
        )
        return cursor.fetchall()

    finally:
        cursor.close()


def row_day(value) -> date:
    """Convert a store-key value into a date."""
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        ).date()

    raise ValueError(
        f"store_key value {value!r} is not a date/datetime"
    )


def split_by_day(
    rows: list[dict[str, Any]],
    store_key: str | None,
) -> dict[date | None, list[dict[str, Any]]]:
    """Split rows into daily partitions using the configured store key."""
    if not store_key:
        return {None: rows}

    result = defaultdict(list)

    for row in rows:
        if row.get(store_key) is None:
            raise ValueError(
                f"store_key column {store_key!r} contains NULL"
            )

        result[row_day(row[store_key])].append(row)

    return result


def yaml_value(value):
    """Convert values into YAML-safe values."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    return value


def upload_rows(
    s3_client,
    s3_config: dict[str, Any],
    table: dict[str, Any],
    rows: list[dict[str, Any]],
) -> int:
    """Write rows to Parquet and upload the files to S3."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    prefix = str(
        s3_config.get("prefix", "")
    ).strip("/")

    table_key = str(
        table["key"]
    ).strip("/")

    base = "/".join(
        part
        for part in (
            prefix,
            table_key,
        )
        if part
    )

    file_count = 0

    daily_groups = split_by_day(
        rows,
        table.get("store_key"),
    )

    for day, day_rows in daily_groups.items():
        directory = base

        if day:
            directory += (
                f"/year={day.year:04d}"
                f"/month={day.month:02d}"
                f"/day={day.day:02d}"
            )

        filename = (
            "part-"
            f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}-"
            f"{uuid.uuid4().hex}.parquet"
        )

        object_key = f"{directory}/{filename}"

        with tempfile.TemporaryDirectory(
            prefix="mysql-export-"
        ) as temp_directory:
            parquet_path = (
                Path(temp_directory) / filename
            )

            arrow_table = pa.Table.from_pylist(
                day_rows
            )

            pq.write_table(
                arrow_table,
                parquet_path,
                compression="snappy",
            )

            s3_client.upload_file(
                str(parquet_path),
                s3_config["bucket"],
                object_key,
            )

        file_count += 1

        LOG.info(
            "Uploaded %d rows to s3://%s/%s",
            len(day_rows),
            s3_config["bucket"],
            object_key,
        )

    return file_count


def export_table(
    db: dict[str, Any],
    s3_config: dict[str, Any],
    s3_client,
    table: dict[str, Any],
    table_path: str,
    batch_size: int,
) -> tuple[int, int, Any]:
    """Export one batch from one table using its resolved database."""
    source_name = get_source_name(db)
    schema_name = db["database"]
    offset = table.get("offset_value")

    LOG.info(
        "Reading source=%s database=%s table=%s "
        "current_offset=%r limit=%d",
        source_name,
        schema_name,
        table["table"],
        offset,
        batch_size,
    )

    connection = connect(db)

    try:
        rows = fetch_batch(
            connection,
            table,
            offset,
            batch_size,
        )

        now = datetime.now(
            timezone.utc
        ).isoformat()

        LOG.info(
            "Fetched %d rows from source=%s table=%s",
            len(rows),
            source_name,
            table["table"],
        )

        if not rows:
            save_table_progress(
                table_path=table_path,
                table=table,
                last_read=now,
            )

            return 0, 0, offset

        file_count = upload_rows(
            s3_client,
            s3_config,
            table,
            rows,
        )

        new_offset = rows[-1][
            table["offset_column"]
        ]

        save_table_progress(
            table_path=table_path,
            table=table,
            last_read=now,
            new_offset=new_offset,
            advance_offset=True,
        )

        return (
            len(rows),
            file_count,
            new_offset,
        )

    finally:
        connection.close()


def create_s3_client(
    s3_config: dict[str, Any],
):
    """
    Create an S3 client using the normal AWS credential chain.

    Supported credential sources include:
    - EC2 IAM role
    - AWS profile
    - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    """
    import boto3

    arguments = {}

    if s3_config.get("region"):
        arguments["region_name"] = s3_config["region"]

    if s3_config.get("endpoint_url"):
        arguments["endpoint_url"] = s3_config["endpoint_url"]

    return boto3.client(
        "s3",
        **arguments,
    )


def load_table_jobs(
    table_path: str,
    databases_by_source: dict[str, dict[str, Any]],
    databases_by_schema: dict[str, list[dict[str, Any]]],
    enabled_databases: list[dict[str, Any]],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Load and validate the currently enabled table jobs."""
    table_document = load_yaml(
        table_path,
        expand_environment=False,
    )
    configured_tables = table_document.get("tables")

    if not isinstance(configured_tables, list):
        raise ValueError(
            "table_config.yml does not contain a 'tables' list."
        )

    tables = []

    for index, item in enumerate(configured_tables, start=1):
        if not isinstance(item, dict):
            raise ValueError(
                f"Table entry #{index} must be a YAML object."
            )

        # Requiring an explicit true makes newly added/manual entries safe by
        # default. discover_tables.py always writes enabled: false for new ones.
        if item.get("enabled", False):
            tables.append(item)

    tables.sort(
        key=lambda item: int(item["order"])
    )

    table_keys = [
        str(item["key"])
        for item in tables
    ]

    if len(set(table_keys)) != len(table_keys):
        raise ValueError(
            "Every enabled table must have a unique key."
        )

    identities = [
        table_identity(item)
        for item in tables
    ]

    if len(set(identities)) != len(identities):
        raise ValueError(
            "Every enabled source/database/table combination must be unique."
        )

    table_jobs = []

    for table in tables:
        db = resolve_table_database(
            table=table,
            by_source=databases_by_source,
            by_schema=databases_by_schema,
            enabled_databases=enabled_databases,
        )
        table_jobs.append((table, db))

    return table_jobs


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export tables from multiple MySQL "
            "databases to S3 Parquet files."
        )
    )

    parser.add_argument(
        "--db",
        default="db.yml",
        help="Path to the database and S3 configuration.",
    )

    parser.add_argument(
        "--tables",
        default="table_config.yml",
        help="Path to the generated table configuration.",
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one export cycle and exit.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(message)s"
        ),
    )

    config = load_yaml(args.db)

    database_configs = get_database_configs(
        config
    )

    (
        databases_by_source,
        databases_by_schema,
        enabled_databases,
    ) = build_database_registry(
        database_configs
    )

    if not enabled_databases:
        LOG.warning(
            "No enabled databases were found."
        )
        return

    if "s3" not in config:
        raise ValueError(
            "The db.yml file does not contain an 's3' section."
        )

    s3_config = config["s3"]

    if not s3_config.get("bucket"):
        raise ValueError(
            "s3.bucket is required."
        )

    configured_batch_size = int(
        config.get("batch_size", 100)
    )

    if configured_batch_size <= 0:
        raise ValueError(
            "batch_size must be greater than zero"
        )

    batch_size = min(
        configured_batch_size,
        100,
    )

    if configured_batch_size > 100:
        LOG.warning(
            "batch_size=%d exceeds the maximum; using 100",
            configured_batch_size,
        )

    sleep_seconds = int(
        config.get("sleep_seconds", 60)
    )

    if sleep_seconds < 0:
        raise ValueError(
            "sleep_seconds cannot be negative"
        )

    table_jobs = load_table_jobs(
        table_path=args.tables,
        databases_by_source=databases_by_source,
        databases_by_schema=databases_by_schema,
        enabled_databases=enabled_databases,
    )

    sources_in_use = {
        get_source_name(db)
        for _, db in table_jobs
    }

    LOG.info(
        "Loaded %d enabled tables across %d database source(s): %s",
        len(table_jobs),
        len(sources_in_use),
        ", ".join(sorted(sources_in_use)),
    )

    s3_client = create_s3_client(
        s3_config
    )

    stopping = False

    def stop(_number, _frame):
        nonlocal stopping
        stopping = True
        LOG.info("Shutdown requested.")

    signal.signal(
        signal.SIGTERM,
        stop,
    )

    signal.signal(
        signal.SIGINT,
        stop,
    )

    while not stopping:
        try:
            table_jobs = load_table_jobs(
                table_path=args.tables,
                databases_by_source=databases_by_source,
                databases_by_schema=databases_by_schema,
                enabled_databases=enabled_databases,
            )
        except Exception:
            LOG.exception(
                "Could not reload %s; retaining the last valid table "
                "configuration for this cycle",
                args.tables,
            )

        LOG.info(
            "Starting export cycle for %d enabled tables",
            len(table_jobs),
        )

        for table, db in table_jobs:
            if stopping:
                break

            source_name = get_source_name(db)

            try:
                count, files, new_offset = export_table(
                    db=db,
                    s3_config=s3_config,
                    s3_client=s3_client,
                    table=table,
                    table_path=args.tables,
                    batch_size=batch_size,
                )

                LOG.info(
                    "Completed source=%s database=%s "
                    "table=%s rows=%d parquet_files=%d "
                    "new_offset=%r",
                    source_name,
                    db["database"],
                    table["table"],
                    count,
                    files,
                    new_offset,
                )

            except Exception:
                LOG.exception(
                    "Failed source=%s database=%s table=%s; "
                    "continuing",
                    source_name,
                    db["database"],
                    table.get("table"),
                )

        if args.once or stopping:
            break

        LOG.info(
            "Cycle complete; sleeping %d seconds",
            sleep_seconds,
        )

        try:
            table_file_stat = Path(args.tables).stat()
            table_file_marker = (
                table_file_stat.st_mtime_ns,
                table_file_stat.st_size,
            )
        except OSError:
            table_file_marker = None

        for _ in range(sleep_seconds):
            if stopping:
                break

            time.sleep(1)

            try:
                current_stat = Path(args.tables).stat()
                current_marker = (
                    current_stat.st_mtime_ns,
                    current_stat.st_size,
                )
            except OSError:
                current_marker = None

            if current_marker != table_file_marker:
                LOG.info(
                    "%s changed; starting the next cycle early",
                    args.tables,
                )
                break

    LOG.info("Exporter stopped.")


if __name__ == "__main__":
    main()
