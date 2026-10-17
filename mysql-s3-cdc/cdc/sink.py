"""Consume Debezium envelopes, filter operations, and write Parquet to S3."""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import tempfile
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, TopicPartition

from .config import (
    TableConfig,
    load_databases,
    load_s3,
    load_tables,
    load_yaml,
)
from .discovery import _write_atomic, file_lock


LOG = logging.getLogger("KafkaS3Archive")
PROGRESS_PATH = Path(os.getenv("CDC_PROGRESS_FILE", "runtime/table-progress.yml"))
BINLOG_NUMBER = re.compile(r"(\d+)$")
OPERATION_NAMES = {"c": "insert", "r": "snapshot", "u": "update", "d": "delete"}


def topic_map() -> dict[str, TableConfig]:
    sources = {
        str(db["name"]): (str(db["topic_prefix"]), str(db["database"]))
        for db in load_databases()
    }
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
    return mapping


def sortable_lsn(source: dict[str, Any]) -> str:
    """Return a lexically sortable MySQL binlog file/position/row coordinate."""
    filename = str(source.get("file") or "")
    match = BINLOG_NUMBER.search(filename)
    file_number = int(match.group(1)) if match else 0
    return f"{file_number:020d}:{int(source.get('pos') or 0):020d}:{int(source.get('row') or 0):010d}"


def event_record(envelope: dict[str, Any], message, table: TableConfig) -> dict[str, Any] | None:
    operation = str(envelope.get("op") or "")
    if not table.accepts(operation):
        return None
    source = envelope.get("source") or {}
    row = envelope.get("before") if operation == "d" else envelope.get("after")
    record = dict(row or {})
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
    return record


def partition_day(record: dict[str, Any], table: TableConfig) -> date:
    value = record.get(table.partition_column) if table.partition_column else None
    if value is None:
        milliseconds = int(record.get("_cdc_event_ts_ms") or 0)
        return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        # Debezium schema-less JSON represents DATE as epoch days, DATETIME
        # as epoch milliseconds or microseconds (depending on precision), and
        # nanosecond mode as epoch nanoseconds. A configured numeric partition
        # column is therefore normalized by magnitude.
        magnitude = abs(float(value))
        if magnitude < 10_000_000:
            return date(1970, 1, 1) + timedelta(days=int(value))
        divisor = 1_000
        if magnitude >= 100_000_000_000_000_000:
            divisor = 1_000_000_000
        elif magnitude >= 100_000_000_000_000:
            divisor = 1_000_000
        return datetime.fromtimestamp(
            float(value) / divisor, timezone.utc
        ).date()
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()


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
        groups: dict[date, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[partition_day(row, table)].append(row)
        prefix = str(self.config.get("prefix", "")).strip("/")
        latest_row = max(rows, key=lambda row: int(row["_cdc_kafka_offset"]))
        latest_day = partition_day(latest_row, table)
        latest_object_key = ""
        for day, day_rows in groups.items():
            start_offset = min(int(row["_cdc_kafka_offset"]) for row in day_rows)
            end_offset = max(int(row["_cdc_kafka_offset"]) for row in day_rows)
            directory = "/".join(part for part in (prefix, table.key.strip("/")) if part)
            directory += f"/year={day.year:04d}/month={day.month:02d}/day={day.day:02d}"
            filename = f"part-p{partition:05d}-o{start_offset:020d}-{end_offset:020d}.parquet"
            object_key = f"{directory}/{filename}"
            with tempfile.TemporaryDirectory(prefix="mysql-cdc-") as temporary:
                path = Path(temporary) / filename
                pq.write_table(pa.Table.from_pylist(normalized_rows(day_rows)), path, compression="snappy")
                self.client.upload_file(str(path), self.config["bucket"], object_key)
            LOG.info(
                "Uploaded %d CDC rows to s3://%s/%s "
                "[topic=%s partition=%d offsets=%d-%d]",
                len(day_rows), self.config["bucket"], object_key,
                topic, partition, start_offset, end_offset,
            )
            if day == latest_day:
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


def flush(consumer: Consumer, writer: S3Writer, buffered: list[tuple[Any, TableConfig, dict[str, Any] | None]]) -> None:
    uploads: dict[tuple[str, int, TableConfig], list[dict[str, Any]]] = defaultdict(list)
    offsets: dict[tuple[str, int], int] = {}
    for message, table, record in buffered:
        key = (message.topic(), message.partition())
        offsets[key] = max(offsets.get(key, -1), message.offset())
        if record is not None:
            uploads[(message.topic(), message.partition(), table)].append(record)
    for (topic, partition, table), rows in uploads.items():
        last_lsn, store_key = writer.upload(table, topic, partition, rows)
        store_table_progress(table, last_lsn, store_key)
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
    last_marker: tuple[int, int] | None = None
    last_missing_topic_warning = 0.0
    mapping: dict[str, TableConfig] = {}
    try:
        while not stopping:
            if not buffered:
                stat = TABLES_PATH.stat()
                marker = (stat.st_mtime_ns, stat.st_size)
                if marker != last_marker:
                    mapping = topic_map()
                    if mapping:
                        consumer.subscribe(sorted(mapping))
                    else:
                        consumer.unsubscribe()
                    last_marker = marker
                    LOG.info("Subscribed to %d enabled table topic(s)", len(mapping))
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
                        record = event_record(envelope, message, table) if envelope else None
                        buffered.append((message, table, record))
            if buffered and (len(buffered) >= batch_size or time.monotonic() - last_flush >= flush_seconds):
                try:
                    flush(consumer, writer, buffered)
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
            flush(consumer, writer, buffered)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

