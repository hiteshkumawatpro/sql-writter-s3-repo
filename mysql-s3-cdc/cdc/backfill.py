"""One-time direct MySQL-to-S3 Parquet backfill for an enabled CDC table."""

from __future__ import annotations

import argparse
import logging
import tempfile
from collections import defaultdict
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any

import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq

from .config import TableConfig, load_databases, load_s3, load_tables
from .sink import S3Writer, normalized_rows, partition_hour


LOG = logging.getLogger("MySQLS3Backfill")


def normalize_mysql_temporals(record: dict[str, Any]) -> dict[str, Any]:
    """Keep MySQL temporal fields as Arrow-compatible logical values."""
    normalized = dict(record)
    for column, value in normalized.items():
        # mysql-connector represents MySQL TIME as timedelta. Arrow needs a
        # native time value. Values outside a clock day remain strings because
        # MySQL TIME also supports durations beyond 24 hours.
        if isinstance(value, timedelta):
            total_micros = int(value.total_seconds() * 1_000_000)
            if 0 <= total_micros < 86_400_000_000:
                hours, remainder = divmod(total_micros, 3_600_000_000)
                minutes, remainder = divmod(remainder, 60_000_000)
                seconds, micros = divmod(remainder, 1_000_000)
                normalized[column] = datetime_time(hours, minutes, seconds, micros)
            else:
                normalized[column] = str(value)
    return normalized


def configured_source(source_name: str) -> dict[str, Any]:
    matches = [db for db in load_databases() if str(db["name"]) == source_name]
    if not matches:
        raise ValueError(f"Unknown or disabled database source: {source_name}")
    return matches[0]


def configured_table(source_name: str, table_name: str) -> TableConfig:
    matches = [
        table for table in load_tables()
        if table.source == source_name and table.table == table_name
    ]
    if not matches:
        raise ValueError(
            f"Table {source_name}.{table_name} is not present in config/tables.yml"
        )
    table = matches[0]
    if not table.enabled:
        raise ValueError(
            f"Table {source_name}.{table_name} is disabled in config/tables.yml"
        )
    return table


def quoted_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def snapshot_metadata(
    row: dict[str, Any],
    table: TableConfig,
    snapshot_time: datetime,
    run_id: str,
) -> dict[str, Any]:
    record = normalize_mysql_temporals(row)
    record.update({
        "lsn": None,
        "_cdc_operation": "snapshot",
        "_cdc_backfill": True,
        "_cdc_backfill_run_id": run_id,
        "_cdc_source": table.source,
        "_cdc_database": table.database,
        "_cdc_table": table.table,
        "_cdc_event_ts_ms": int(snapshot_time.timestamp() * 1000),
        "_cdc_binlog_file": None,
        "_cdc_binlog_position": None,
        "_cdc_binlog_row": None,
        "_cdc_gtid": None,
        "_cdc_transaction_id": None,
        "_cdc_kafka_topic": None,
        "_cdc_kafka_partition": None,
        "_cdc_kafka_offset": None,
    })
    return record


def upload_batch(
    writer: S3Writer,
    table: TableConfig,
    rows: list[dict[str, Any]],
    batch_number: int,
    run_id: str,
) -> tuple[int, int]:
    groups: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[partition_hour(row, table)].append(row)

    prefix = str(writer.config.get("prefix") or "").strip("/")
    uploaded_rows = 0
    uploaded_files = 0
    for hour, hour_rows in groups.items():
        directory = "/".join(
            part for part in (prefix, table.key.strip("/")) if part
        )
        directory += (
            f"/year={hour.year:04d}/month={hour.month:02d}/"
            f"day={hour.day:02d}/hour={hour.hour:02d}"
        )
        filename = f"backfill-{run_id}-b{batch_number:08d}.snappy.parquet"
        object_key = f"{directory}/{filename}"
        with tempfile.TemporaryDirectory(prefix="mysql-cdc-backfill-") as temporary:
            path = Path(temporary) / filename
            pq.write_table(
                pa.Table.from_pylist(normalized_rows(hour_rows)),
                path,
                compression="snappy",
            )
            writer.client.upload_file(
                str(path), writer.config["bucket"], object_key
            )
        uploaded_rows += len(hour_rows)
        uploaded_files += 1
        LOG.info(
            "Uploaded %d BACKFILL rows to s3://%s/%s",
            len(hour_rows), writer.config["bucket"], object_key,
        )
    return uploaded_rows, uploaded_files


def backfill(
    source_name: str,
    table_name: str,
    batch_size: int,
    run_id: str,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch-size must be positive")
    allowed_run_id = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
    if not run_id or any(character not in allowed_run_id for character in run_id):
        raise ValueError("run-id may contain only letters, numbers, hyphens, and underscores")

    db = configured_source(source_name)
    table = configured_table(source_name, table_name)
    if table.database != str(db["database"]):
        raise ValueError(
            f"Table database {table.database!r} does not match source database "
            f"{db['database']!r}"
        )

    s3 = load_s3()["s3"]
    writer = S3Writer(s3)
    connection_arguments: dict[str, Any] = {
        "host": db["host"],
        "port": int(db.get("port", 3306)),
        "user": db["username"],
        "password": db["password"],
        "database": db["database"],
        "connection_timeout": 15,
    }
    if str(db.get("ssl_mode", "preferred")).lower() == "disabled":
        connection_arguments["ssl_disabled"] = True

    connection = mysql.connector.connect(**connection_arguments)
    cursor = None
    snapshot_time = datetime.now(timezone.utc)
    total_rows = 0
    total_files = 0
    try:
        connection.start_transaction(
            isolation_level="REPEATABLE READ",
            readonly=True,
            consistent_snapshot=True,
        )
        count_cursor = connection.cursor()
        try:
            count_cursor.execute(
                f"SELECT COUNT(*) FROM {quoted_identifier(table.database)}."
                f"{quoted_identifier(table.table)}"
            )
            expected_rows = int(count_cursor.fetchone()[0])
        finally:
            count_cursor.close()

        LOG.info(
            "Starting consistent backfill of %s.%s: %d source rows, "
            "batch_size=%d, run_id=%s",
            table.database, table.table, expected_rows, batch_size, run_id,
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            f"SELECT * FROM {quoted_identifier(table.database)}."
            f"{quoted_identifier(table.table)}"
        )
        batch_number = 0
        while True:
            source_rows = cursor.fetchmany(batch_size)
            if not source_rows:
                break
            batch_number += 1
            records = [
                snapshot_metadata(dict(row), table, snapshot_time, run_id)
                for row in source_rows
            ]
            row_count, file_count = upload_batch(
                writer, table, records, batch_number, run_id
            )
            total_rows += row_count
            total_files += file_count
            LOG.info(
                "Backfill progress: %d/%d rows uploaded in %d file(s)",
                total_rows, expected_rows, total_files,
            )
        connection.commit()
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()

    if total_rows != expected_rows:
        raise RuntimeError(
            f"Backfill row-count mismatch: source={expected_rows}, uploaded={total_rows}"
        )
    LOG.info(
        "Backfill complete: %d rows uploaded in %d file(s) for %s.%s",
        total_rows, total_files, table.database, table.table,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill one enabled MySQL table directly to S3 Parquet."
    )
    parser.add_argument("source", help="Source name from config/databases.yml")
    parser.add_argument("table", help="Table name from config/tables.yml")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument(
        "--run-id",
        default="manual-v1",
        help="Stable identifier used in S3 object names (default: manual-v1).",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    backfill(args.source, args.table, args.batch_size, args.run_id)


if __name__ == "__main__":
    main()
