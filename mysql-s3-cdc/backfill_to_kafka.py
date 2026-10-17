"""Publish one MySQL table as Debezium-style snapshot events to Kafka.

Run this from the mysql-s3-cdc directory while Kafka and the S3 sink are
running. The existing sink consumes the generated records and uploads them to
the table's configured S3 location.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import mysql.connector
from confluent_kafka import Producer

from cdc.config import TableConfig, load_databases, load_tables


EPOCH_DATE = date(1970, 1, 1)
EPOCH_DATETIME = datetime(1970, 1, 1)


@dataclass(frozen=True)
class ColumnType:
    data_type: str
    datetime_precision: int | None


def source_config(source_name: str) -> dict[str, Any]:
    for source in load_databases():
        if str(source["name"]) == source_name:
            return source
    raise ValueError(f"Unknown or disabled source: {source_name}")


def table_config(source_name: str, table_name: str) -> TableConfig:
    for table in load_tables():
        if table.source == source_name and table.table == table_name:
            if not table.enabled:
                raise ValueError(
                    f"{source_name}.{table_name} is disabled in config/tables.yml"
                )
            return table
    raise ValueError(
        f"{source_name}.{table_name} is missing from config/tables.yml"
    )


def quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def elapsed_microseconds(value: datetime) -> int:
    delta = value - EPOCH_DATETIME
    return (
        (delta.days * 86400 + delta.seconds) * 1_000_000
        + delta.microseconds
    )


def time_microseconds(value: time | timedelta) -> int:
    if isinstance(value, timedelta):
        return (
            (value.days * 86400 + value.seconds) * 1_000_000
            + value.microseconds
        )
    return (
        ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000
        + value.microsecond
    )


def utc_timestamp(value: datetime, connection_time_zone: str) -> str:
    try:
        zone = ZoneInfo(connection_time_zone)
    except ZoneInfoNotFoundError as exc:
        if connection_time_zone in {"Asia/Calcutta", "Asia/Kolkata"}:
            # India has used UTC+05:30 without daylight-saving changes since
            # 1945, so this fallback is deterministic when Windows lacks the
            # optional Python tzdata package.
            zone = timezone(timedelta(hours=5, minutes=30))
        elif connection_time_zone.upper() in {"UTC", "ETC/UTC"}:
            zone = timezone.utc
        else:
            raise ValueError(
                f"Unknown connection_time_zone {connection_time_zone!r}; "
                "install system time-zone data or correct databases.yml"
            ) from exc
    aware = value if value.tzinfo is not None else value.replace(tzinfo=zone)
    result = aware.astimezone(timezone.utc).isoformat()
    return result.replace("+00:00", "Z")


def json_value(
    value: Any,
    column: ColumnType | None = None,
    connection_time_zone: str = "UTC",
) -> Any:
    """Match Debezium's default schema-less JSON representation for MySQL."""
    data_type = column.data_type if column else ""
    precision = column.datetime_precision if column else None
    if value is None:
        return None
    if data_type == "date" and isinstance(value, date):
        return (value - EPOCH_DATE).days
    if data_type == "time" and isinstance(value, (time, timedelta)):
        return time_microseconds(value)
    if data_type == "datetime" and isinstance(value, datetime):
        microseconds = elapsed_microseconds(value.replace(tzinfo=None))
        return microseconds if (precision or 0) > 3 else microseconds // 1000
    if data_type == "timestamp" and isinstance(value, datetime):
        return utc_timestamp(value, connection_time_zone)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return str(value)


def envelope(
    row: dict[str, Any],
    source: dict[str, Any],
    table: TableConfig,
    row_number: int,
    snapshot_ms: int,
    run_id: str,
    column_types: dict[str, ColumnType],
) -> dict[str, Any]:
    connection_time_zone = str(source.get("connection_time_zone") or "UTC")
    after = {
        name: json_value(value, column_types.get(name), connection_time_zone)
        for name, value in row.items()
    }
    # Reserved metadata is intentionally included inside `after` so it survives
    # the normal sink conversion into Parquet.
    after["_cdc_backfill"] = True
    after["_cdc_backfill_run_id"] = run_id
    return {
        "before": None,
        "after": after,
        "source": {
            "version": "manual-kafka-backfill-v1",
            "connector": "mysql",
            "name": str(source["topic_prefix"]),
            "ts_ms": snapshot_ms,
            "snapshot": "true",
            "db": table.database,
            "table": table.table,
            "server_id": int(source["server_id"]),
            "file": "backfill.000000",
            "pos": row_number,
            "row": 0,
        },
        "op": "r",
        "ts_ms": snapshot_ms,
        "transaction": None,
    }


def run(source_name: str, table_name: str, batch_size: int, run_id: str) -> None:
    if batch_size <= 0:
        raise ValueError("batch-size must be positive")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
    if not run_id or any(character not in allowed for character in run_id):
        raise ValueError(
            "run-id may contain only letters, numbers, hyphens, and underscores"
        )

    source = source_config(source_name)
    table = table_config(source_name, table_name)
    if table.database != str(source["database"]):
        raise ValueError("The table and source database definitions do not match")

    topic = f"{source['topic_prefix']}.{table.database}.{table.table}"
    bootstrap = os.getenv("CDC_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": f"mysql-s3-backfill-{run_id}",
        "enable.idempotence": True,
        "acks": "all",
    })

    connection_options: dict[str, Any] = {
        "host": source["host"],
        "port": int(source.get("port", 3306)),
        "user": source["username"],
        "password": source["password"],
        "database": source["database"],
        "connection_timeout": 15,
    }
    if str(source.get("ssl_mode", "preferred")).lower() == "disabled":
        connection_options["ssl_disabled"] = True

    connection = mysql.connector.connect(**connection_options)
    cursor = None
    delivered = 0
    delivery_errors: list[str] = []

    def delivery_report(error, _message) -> None:
        nonlocal delivered
        if error is not None:
            delivery_errors.append(str(error))
        else:
            delivered += 1

    try:
        connection.start_transaction(
            isolation_level="REPEATABLE READ",
            readonly=True,
            consistent_snapshot=True,
        )
        count_cursor = connection.cursor()
        try:
            count_cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, DATETIME_PRECISION "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
                (table.database, table.table),
            )
            column_types = {
                str(name): ColumnType(
                    data_type=str(data_type).lower(),
                    datetime_precision=(
                        int(datetime_precision)
                        if datetime_precision is not None else None
                    ),
                )
                for name, data_type, datetime_precision in count_cursor.fetchall()
            }
            if not column_types:
                raise ValueError(
                    f"MySQL table {table.database}.{table.table} was not found"
                )
            count_cursor.execute(
                f"SELECT COUNT(*) FROM {quote_identifier(table.database)}."
                f"{quote_identifier(table.table)}"
            )
            expected = int(count_cursor.fetchone()[0])
        finally:
            count_cursor.close()

        print(
            f"Publishing {expected} rows from {table.database}.{table.table} "
            f"to Kafka topic {topic} via {bootstrap}",
            flush=True,
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            f"SELECT * FROM {quote_identifier(table.database)}."
            f"{quote_identifier(table.table)}"
        )
        snapshot_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        read_rows = 0
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                read_rows += 1
                payload = envelope(
                    dict(row), source, table, read_rows, snapshot_ms, run_id,
                    column_types,
                )
                while True:
                    try:
                        producer.produce(
                            topic,
                            key=None,
                            value=json.dumps(
                                payload, separators=(",", ":"), ensure_ascii=False
                            ).encode("utf-8"),
                            on_delivery=delivery_report,
                        )
                        break
                    except BufferError:
                        producer.poll(1)
                producer.poll(0)
            producer.flush(60)
            if delivery_errors:
                raise RuntimeError(
                    "Kafka delivery failed: " + "; ".join(delivery_errors[:3])
                )
            print(
                f"Kafka backfill progress: {delivered}/{expected} rows delivered",
                flush=True,
            )
        connection.commit()
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()
        producer.flush(10)

    if delivered != expected:
        raise RuntimeError(
            f"Kafka row-count mismatch: source={expected}, delivered={delivered}"
        )
    print(
        f"Kafka backfill complete: {delivered} snapshot rows delivered to {topic}. "
        "The existing sink will upload them to S3.",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Publish one enabled MySQL table to its normal CDC Kafka topic so "
            "the existing sink archives it to S3."
        )
    )
    parser.add_argument("source", help="Source name from config/databases.yml")
    parser.add_argument("table", help="Enabled table name from config/tables.yml")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--run-id", default="manual-v1")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm that snapshot records should be produced to Kafka.",
    )
    args = parser.parse_args()
    if not args.yes:
        parser.error("this publishes records to Kafka; rerun with --yes")
    run(args.source, args.table, args.batch_size, args.run_id)


if __name__ == "__main__":
    main()
