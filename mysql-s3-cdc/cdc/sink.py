"""Consume Debezium envelopes, filter operations, and write Parquet to S3."""

from __future__ import annotations
# fix table path
import json
import hashlib
import logging
import os
import re
import signal
import sqlite3
import tempfile
import time
from collections import defaultdict
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, TopicPartition

from .config import (
    TABLES_PATH,
    TableConfig,
    load_databases,
    load_s3,
    load_tables,
    load_yaml,
)
from .discovery import _write_atomic, file_lock
from .runtime_state import RUNTIME, STATE_PATH, active_replacements, record_snapshot_rows


LOG = logging.getLogger("KafkaS3Archive")
PROGRESS_PATH = Path(os.getenv("CDC_PROGRESS_FILE", "runtime/table-progress.yml"))
BINLOG_NUMBER = re.compile(r"(\d+)$")
OPERATION_NAMES = {"c": "insert", "r": "snapshot", "u": "update", "d": "delete"}


class SchemaEvolutionError(RuntimeError):
    """A record batch cannot be represented by one Parquet schema."""


class EventLedger:
    """Durable event-ID ledger used to suppress connector/restart replays."""

    def __init__(self) -> None:
        path = RUNTIME / "reconciliation" / "event-manifest.sqlite3"
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=30)
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS events (source TEXT, table_name TEXT, "
            "event_id TEXT, object_key TEXT, occurrences INTEGER NOT NULL, "
            "PRIMARY KEY(source, table_name, event_id))"
        )
        self.connection.commit()

    def unseen(self, table: TableConfig, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result = []
        for row in rows:
            event_id = str(row.get("_cdc_event_id") or "")
            exists = self.connection.execute(
                "SELECT 1 FROM events WHERE source=? AND table_name=? AND event_id=?",
                (table.source, table.table, event_id),
            ).fetchone()
            if not exists:
                result.append(row)
        return result

    def record(self, table: TableConfig, rows: list[dict[str, Any]], object_key: str) -> None:
        self.connection.executemany(
            "INSERT INTO events VALUES (?, ?, ?, ?, 1) "
            "ON CONFLICT(source, table_name, event_id) DO UPDATE SET "
            "occurrences=occurrences+1",
            [
                (table.source, table.table, str(row["_cdc_event_id"]), object_key)
                for row in rows
            ],
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def topic_map() -> dict[str, TableConfig]:
    sources = {
        str(db["name"]): (str(db["topic_prefix"]), str(db["database"]))
        for db in load_databases()
    }
    replacements = active_replacements()
    mapping = {}
    for table in load_tables():
        if not table.enabled:
            continue
        source = sources.get(table.source)
        if source is None:
            LOG.warning(
                "Ignoring enabled table %s.%s because its source is unknown "
                "or disabled",
                table.source,
                table.table,
            )
            continue
        prefix, configured_database = source
        if table.database != configured_database:
            LOG.warning(
                "Ignoring enabled table %s.%s because database=%s does not "
                "match configured database=%s",
                table.source,
                table.table,
                table.database,
                configured_database,
            )
            continue
        mapping[f"{prefix}.{table.database}.{table.table}"] = table
        for replacement in replacements:
            if replacement.get("source") == table.source:
                mapping[
                    f"{replacement['topic_prefix']}.{table.database}.{table.table}"
                ] = table
    return mapping


def sortable_lsn(source: dict[str, Any]) -> str:
    """Return a lexically sortable MySQL binlog file/position/row coordinate."""
    filename = str(source.get("file") or "")
    match = BINLOG_NUMBER.search(filename)
    file_number = int(match.group(1)) if match else 0
    return f"{file_number:020d}:{int(source.get('pos') or 0):020d}:{int(source.get('row') or 0):010d}"


def temporal_columns() -> dict[tuple[str, str, str], dict[str, str]]:
    """Read MySQL temporal column types so schema-less events can be normalized."""
    result: dict[tuple[str, str, str], dict[str, str]] = {}
    tables = load_tables()
    for db in load_databases():
        source = str(db["name"])
        enabled_names = {
            table.table for table in tables
            if table.enabled and table.source == source
            and table.database == str(db["database"])
        }
        if not enabled_names:
            continue
        arguments: dict[str, Any] = {
            "host": db["host"], "port": int(db.get("port", 3306)),
            "user": db["username"], "password": db["password"],
            "database": db["database"], "connection_timeout": 15,
        }
        if str(db.get("ssl_mode", "preferred")).lower() == "disabled":
            arguments["ssl_disabled"] = True
        connection = None
        cursor = None
        try:
            connection = mysql.connector.connect(**arguments)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s "
                "AND DATA_TYPE IN ('date','datetime','timestamp','time')",
                (str(db["database"]),),
            )
            for table_name, column_name, data_type in cursor.fetchall():
                if str(table_name) in enabled_names:
                    result.setdefault(
                        (source, str(db["database"]), str(table_name)), {}
                    )[str(column_name)] = str(data_type).lower()
        except Exception as exc:
            LOG.warning("Could not inspect temporal columns for %s: %s", source, exc)
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()
    return result


def normalize_temporal_value(value: Any, data_type: str) -> Any:
    if value is None:
        return value
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    if isinstance(value, datetime_time):
        return value.replace(microsecond=0)
    if isinstance(value, date):
        return value
    if data_type == "date":
        if isinstance(value, (int, float)):
            return date(1970, 1, 1) + timedelta(days=int(value))
        return date.fromisoformat(str(value).rstrip("Z"))
    if data_type == "time":
        if isinstance(value, (int, float)):
            micros = int(value)
            hours, micros = divmod(micros, 3_600_000_000)
            minutes, micros = divmod(micros, 60_000_000)
            seconds, micros = divmod(micros, 1_000_000)
            return datetime_time(hours % 24, minutes, seconds)
        return datetime_time.fromisoformat(str(value).rstrip("Z")).replace(
            microsecond=0
        )
    if data_type == "datetime":
        if isinstance(value, (int, float)):
            divisor = 1_000_000 if abs(float(value)) >= 100_000_000_000_000 else 1_000
            return datetime.fromtimestamp(
                float(value) / divisor, timezone.utc
            ).replace(tzinfo=None, microsecond=0)
        return datetime.fromisoformat(
            str(value).rstrip("Z").replace(" ", "T")
        ).replace(microsecond=0)
    # MySQL TIMESTAMP is emitted by Debezium as an ISO UTC ZonedTimestamp.
    if isinstance(value, (int, float)):
        magnitude = abs(float(value))
        divisor = 1_000
        if magnitude >= 100_000_000_000_000_000:
            divisor = 1_000_000_000
        elif magnitude >= 100_000_000_000_000:
            divisor = 1_000_000
        return datetime.fromtimestamp(float(value) / divisor, timezone.utc).replace(
            microsecond=0
        )
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00").replace(" ", "T"))
    parsed = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return parsed.replace(microsecond=0)


def normalize_temporal_record(
    record: dict[str, Any], table: TableConfig,
    columns: dict[tuple[str, str, str], dict[str, str]],
) -> None:
    for column, data_type in columns.get(
        (table.source, table.database, table.table), {}
    ).items():
        if column in record:
            record[column] = normalize_temporal_value(record[column], data_type)


def event_record(
    envelope: dict[str, Any], message, table: TableConfig,
    columns: dict[tuple[str, str, str], dict[str, str]] | None = None,
) -> dict[str, Any] | None:
    operation = str(envelope.get("op") or "")
    if not table.accepts(operation):
        return None
    source = envelope.get("source") or {}
    row = envelope.get("before") if operation == "d" else envelope.get("after")
    record = dict(row or {})
    if columns:
        normalize_temporal_record(record, table, columns)
    # CDC fields deliberately win if a source table uses one of these reserved names.
    record.update({
        "lsn": sortable_lsn(source),
        "_cdc_operation": OPERATION_NAMES.get(operation, operation),
        "_cdc_source": table.source,
        "_cdc_database": table.database,
        "_cdc_table": table.table,
        "_cdc_event_ts_ms": int(source.get("ts_ms") or envelope.get("ts_ms") or 0),
        "_cdc_binlog_file": source.get("file"),
        "_cdc_binlog_position": source.get("pos"),
        "_cdc_binlog_row": source.get("row"),
        "_cdc_gtid": source.get("gtid"),
        "_cdc_transaction_id": (envelope.get("transaction") or {}).get("id"),
        "_cdc_kafka_topic": message.topic(),
        "_cdc_kafka_partition": message.partition(),
        "_cdc_kafka_offset": message.offset(),
    })
    coordinate = ":".join(str(part) for part in (
        table.source, table.database, table.table, source.get("file"),
        source.get("pos"), source.get("row"),
        OPERATION_NAMES.get(operation, operation),
    ))
    # Live events replayed through a recovery connector retain their MySQL
    # coordinate and therefore the same event ID. Snapshot rows do not have a
    # portable cross-connector row identity, so Kafka coordinates are used.
    if operation == "r" or not source.get("file"):
        coordinate = f"{message.topic()}:{message.partition()}:{message.offset()}"
    record["_cdc_event_id"] = hashlib.sha256(coordinate.encode()).hexdigest()
    return record


def partition_hour(record: dict[str, Any], table: TableConfig) -> datetime:
    def floor_to_hour(value: datetime) -> datetime:
        return value.replace(minute=0, second=0, microsecond=0)

    value = record.get(table.partition_column) if table.partition_column else None
    if value is None:
        milliseconds = int(record.get("_cdc_event_ts_ms") or 0)
        event_time = datetime.fromtimestamp(milliseconds / 1000, timezone.utc)
        if table.partition_column:
            return event_time.replace(hour=1, minute=0, second=0, microsecond=0)
        return floor_to_hour(event_time)
    if isinstance(value, datetime):
        return floor_to_hour(value)
    if isinstance(value, date):
        return datetime.combine(value, datetime_time(hour=1))
    if isinstance(value, (int, float)):
        # Debezium schema-less JSON represents DATE as epoch days, DATETIME
        # as epoch milliseconds or microseconds (depending on precision), and
        # nanosecond mode as epoch nanoseconds. A configured numeric partition
        # column is therefore normalized by magnitude.
        magnitude = abs(float(value))
        if magnitude < 10_000_000:
            day = date(1970, 1, 1) + timedelta(days=int(value))
            return datetime.combine(day, datetime_time(hour=1))
        divisor = 1_000
        if magnitude >= 100_000_000_000_000_000:
            divisor = 1_000_000_000
        elif magnitude >= 100_000_000_000_000:
            divisor = 1_000_000
        return floor_to_hour(datetime.fromtimestamp(float(value) / divisor, timezone.utc))
    text = str(value)
    if "T" not in text and " " not in text:
        parsed_date = date.fromisoformat(text.rstrip("Z"))
        return datetime.combine(parsed_date, datetime_time(hour=1))
    return floor_to_hour(datetime.fromisoformat(text.replace("Z", "+00:00")))


def partition_day(record: dict[str, Any], table: TableConfig) -> date:
    """Backward-compatible helper for callers that need only the calendar day."""
    return partition_hour(record, table).date()


def normalized_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Make schema-evolved rows share the union of all keys in this file."""
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for field in row:
            if field not in seen:
                seen.add(field)
                fields.append(field)
    return [{field: row.get(field) for field in fields} for row in rows]


class S3Writer:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        credentials = config.get("credentials") or {}
        arguments: dict[str, Any] = {}
        mappings = {
            "region": "region_name", "endpoint_url": "endpoint_url",
        }
        for source, target in mappings.items():
            if config.get(source):
                arguments[target] = config[source]
        credential_mappings = {
            "access_key_id": "aws_access_key_id",
            "secret_access_key": "aws_secret_access_key",
            "session_token": "aws_session_token",
        }
        for source, target in credential_mappings.items():
            if credentials.get(source):
                arguments[target] = credentials[source]
        self.client = boto3.client("s3", **arguments)

    def upload(
        self,
        table: TableConfig,
        topic: str,
        partition: int,
        rows: list[dict[str, Any]],
    ) -> tuple[str, str]:
        groups: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[partition_hour(row, table)].append(row)
        prefix = str(self.config.get("prefix", "")).strip("/")
        latest_row = max(rows, key=lambda row: int(row["_cdc_kafka_offset"]))
        latest_hour = partition_hour(latest_row, table)
        latest_object_key = ""
        for hour, hour_rows in groups.items():
            start_offset = min(int(row["_cdc_kafka_offset"]) for row in hour_rows)
            end_offset = max(int(row["_cdc_kafka_offset"]) for row in hour_rows)
            directory = "/".join(part for part in (prefix, table.key.strip("/")) if part)
            directory += (
                f"/year={hour.year:04d}/month={hour.month:02d}/"
                f"day={hour.day:02d}/hour={hour.hour:02d}"
            )
            topic_token = hashlib.sha256(topic.encode()).hexdigest()[:12]
            filename = (
                f"part-t{topic_token}-p{partition:05d}-"
                f"o{start_offset:020d}-{end_offset:020d}.parquet"
            )
            object_key = f"{directory}/{filename}"
            with tempfile.TemporaryDirectory(prefix="mysql-cdc-") as temporary:
                path = Path(temporary) / filename
                try:
                    arrow_table = pa.Table.from_pylist(normalized_rows(hour_rows))
                    pq.write_table(arrow_table, path, compression="snappy")
                except (pa.ArrowException, TypeError, ValueError, OverflowError) as exc:
                    raise SchemaEvolutionError(
                        f"Cannot create Parquet for {table.source}.{table.table}: {exc}"
                    ) from exc
                self.client.upload_file(str(path), self.config["bucket"], object_key)
            LOG.info(
                "Uploaded %d CDC rows to s3://%s/%s "
                "[topic=%s partition=%d offsets=%d-%d]",
                len(hour_rows), self.config["bucket"], object_key,
                topic, partition, start_offset, end_offset,
            )
            if hour == latest_hour:
                latest_object_key = object_key
        return str(latest_row["lsn"]), latest_object_key


def store_table_progress(table: TableConfig, last_lsn: str, store_key: str) -> None:
    """Record upload progress in ignored runtime state, never tracked config."""
    identity = f"{table.source}.{table.database}.{table.table}"
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(PROGRESS_PATH):
        document = load_yaml(PROGRESS_PATH, expand_environment=False) if PROGRESS_PATH.exists() else {}
        entries = document.setdefault("tables", {})
        current = entries.get(identity) or {}
        current_lsn = current.get("last_lsn")
        # Fixed-width LSNs compare in binlog file/position/row order.
        if current_lsn is None or last_lsn >= str(current_lsn):
            entries[identity] = {
                "last_lsn": last_lsn,
                "store_key": store_key,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            _write_atomic(PROGRESS_PATH, document)


def quarantine_schema_batch(
    table: TableConfig, topic: str, partition: int,
    rows: list[dict[str, Any]], error: Exception,
) -> None:
    """Durably isolate a malformed schema batch so other topics keep flowing."""
    directory = Path(os.getenv("CDC_QUARANTINE_DIR", "runtime/quarantine"))
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{table.source}-{table.table}.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps({
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
                "error": str(error), "topic": topic, "partition": partition,
                "record": row,
            }, default=str, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    LOG.error(
        "Quarantined %d rows for schema evolution error in %s.%s at %s; "
        "other topic partitions will continue",
        len(rows), table.source, table.table, path,
    )


def flush(
    consumer: Consumer, writer: S3Writer,
    buffered: list[tuple[Any, TableConfig, dict[str, Any] | None]],
    ledger: EventLedger,
) -> None:
    uploads: dict[tuple[str, int, TableConfig], list[dict[str, Any]]] = defaultdict(list)
    offsets: dict[tuple[str, int], int] = {}
    for message, table, record in buffered:
        key = (message.topic(), message.partition())
        offsets[key] = max(offsets.get(key, -1), message.offset())
        if record is not None:
            uploads[(message.topic(), message.partition(), table)].append(record)
    for (topic, partition, table), rows in uploads.items():
        original_count = len(rows)
        rows = ledger.unseen(table, rows)
        if len(rows) != original_count:
            LOG.warning(
                "Suppressed %d already-archived CDC event(s) for %s.%s",
                original_count - len(rows), table.source, table.table,
            )
        if not rows:
            continue
        try:
            last_lsn, store_key = writer.upload(table, topic, partition, rows)
        except SchemaEvolutionError as exc:
            quarantine_schema_batch(table, topic, partition, rows, exc)
            continue
        store_table_progress(table, last_lsn, store_key)
        ledger.record(table, rows, store_key)
        snapshot_rows = sum(row.get("_cdc_operation") == "snapshot" for row in rows)
        streaming = any(row.get("_cdc_operation") != "snapshot" for row in rows)
        record_snapshot_rows(table.source, table.table, snapshot_rows, streaming)
    commit_offsets = [TopicPartition(topic, partition, offset + 1) for (topic, partition), offset in offsets.items()]
    if commit_offsets:
        consumer.commit(offsets=commit_offsets, asynchronous=False)
    LOG.info("Flushed %d Kafka records into %d table/partition batches", len(buffered), len(uploads))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    document = load_s3()
    sink_config = document["sink"]
    writer = S3Writer(document["s3"])
    ledger = EventLedger()
    consumer = Consumer({
        "bootstrap.servers": os.getenv("CDC_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
        "group.id": str(sink_config.get("consumer_group", "mysql-s3-cdc-sink-v1")),
        "enable.auto.commit": False,
        "auto.offset.reset": str(sink_config.get("auto_offset_reset", "earliest")),
    })
    stopping = False

    def stop(_number, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    batch_size = int(sink_config.get("batch_size", 1000))
    flush_seconds = float(sink_config.get("flush_seconds", 30))
    upload_retry_seconds = float(sink_config.get("upload_retry_seconds", 30))
    buffered: list[tuple[Any, TableConfig, dict[str, Any] | None]] = []
    last_flush = time.monotonic()
    last_marker: tuple[int, ...] | None = None
    last_missing_topic_warning = 0.0
    last_temporal_refresh = 0.0
    mapping: dict[str, TableConfig] = {}
    columns: dict[tuple[str, str, str], dict[str, str]] = {}
    try:
        while not stopping:
            if not buffered:
                stat = TABLES_PATH.stat()
                control_marker = (0, 0)
                if STATE_PATH.exists():
                    control = STATE_PATH.stat()
                    control_marker = (control.st_mtime_ns, control.st_size)
                marker = (stat.st_mtime_ns, stat.st_size, *control_marker)
                if marker != last_marker:
                    mapping = topic_map()
                    columns = temporal_columns()
                    last_temporal_refresh = time.monotonic()
                    if mapping:
                        consumer.subscribe(sorted(mapping))
                    else:
                        consumer.unsubscribe()
                    last_marker = marker
                    LOG.info(
                        "Subscribed to %d enabled table topic(s); loaded temporal "
                        "metadata for %d table(s)",
                        len(mapping), len(columns),
                    )
                elif time.monotonic() - last_temporal_refresh >= 300:
                    # Refresh independently of tables.yml so an ALTER TABLE
                    # adding a temporal column is picked up while running.
                    columns = temporal_columns()
                    last_temporal_refresh = time.monotonic()
            message = consumer.poll(1.0)
            if message is not None:
                if message.error():
                    error_code = message.error().code()
                    if error_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        # Debezium creates a table topic only when its snapshot
                        # or first change event begins. Keep polling during that
                        # normal gap instead of terminating the S3 sink.
                        now = time.monotonic()
                        if now - last_missing_topic_warning >= 30:
                            LOG.warning(
                                "Waiting for Debezium to create an enabled table topic: %s",
                                message.error(),
                            )
                            last_missing_topic_warning = now
                    elif error_code != KafkaError._PARTITION_EOF:
                        raise RuntimeError(str(message.error()))
                else:
                    table = mapping.get(message.topic())
                    if table is not None:
                        envelope = json.loads(message.value()) if message.value() else None
                        record = (
                            event_record(envelope, message, table, columns)
                            if envelope else None
                        )
                        buffered.append((message, table, record))
            if buffered and (len(buffered) >= batch_size or time.monotonic() - last_flush >= flush_seconds):
                try:
                    flush(consumer, writer, buffered, ledger)
                except Exception:
                    # Keep the uncommitted batch in memory so a temporary S3
                    # outage cannot lose Kafka records or terminate the sink.
                    LOG.exception(
                        "S3 flush failed; retaining %d Kafka record(s) and retrying in %.1f seconds",
                        len(buffered), upload_retry_seconds,
                    )
                    time.sleep(upload_retry_seconds)
                else:
                    buffered.clear()
                    last_flush = time.monotonic()
        if buffered:
            flush(consumer, writer, buffered, ledger)
    finally:
        consumer.close()
        ledger.close()


if __name__ == "__main__":
    main()

