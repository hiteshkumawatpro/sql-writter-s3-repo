"""Build a durable S3 event manifest and report duplicate CDC event IDs."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from .config import load_s3
from .operations import table_config
from .runtime_state import RUNTIME
from .sink import S3Writer


def reconcile(source: str, table_name: str) -> bool:
    _db, table = table_config(source, table_name)
    document = load_s3()
    s3 = document["s3"]
    client = S3Writer(s3).client
    table_key = table.key.strip("/")
    root = "/".join(
        part for part in (str(s3.get("prefix", "")).strip("/"), table_key) if part
    ) + "/"
    directory = RUNTIME / "reconciliation"
    directory.mkdir(parents=True, exist_ok=True)
    database = directory / "event-manifest.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE IF NOT EXISTS events (source TEXT, table_name TEXT, event_id TEXT, "
        "object_key TEXT, occurrences INTEGER NOT NULL, PRIMARY KEY(source, table_name, event_id))"
    )
    connection.execute("DELETE FROM events WHERE source=? AND table_name=?", (source, table_name))
    objects = rows = without_id = 0
    token = None
    try:
        while True:
            arguments = {"Bucket": s3["bucket"], "Prefix": root}
            if token:
                arguments["ContinuationToken"] = token
            response = client.list_objects_v2(**arguments)
            for item in response.get("Contents", []):
                key = item["Key"]
                if not key.endswith(".parquet"):
                    continue
                objects += 1
                with tempfile.TemporaryDirectory(prefix="cdc-reconcile-") as temporary:
                    path = Path(temporary) / "part.parquet"
                    client.download_file(s3["bucket"], key, str(path))
                    parquet = pq.ParquetFile(path)
                    available = set(parquet.schema.names)
                    if "_cdc_event_id" not in available:
                        without_id += parquet.metadata.num_rows
                        continue
                    coordinate_columns = [
                        "_cdc_source", "_cdc_database", "_cdc_table",
                        "_cdc_binlog_file", "_cdc_binlog_position",
                        "_cdc_binlog_row", "_cdc_operation",
                    ]
                    columns = ["_cdc_event_id"] + [
                        column for column in coordinate_columns if column in available
                    ]
                    for batch in parquet.iter_batches(columns=columns):
                        for row in batch.to_pylist():
                            event_id = row.get("_cdc_event_id")
                            if (
                                row.get("_cdc_operation") != "snapshot"
                                and row.get("_cdc_binlog_file")
                                and all(column in row for column in coordinate_columns)
                            ):
                                coordinate = ":".join(str(row.get(column)) for column in coordinate_columns)
                                event_id = hashlib.sha256(coordinate.encode()).hexdigest()
                            rows += 1
                            if event_id is None:
                                without_id += 1
                                continue
                            connection.execute(
                                "INSERT INTO events VALUES (?, ?, ?, ?, 1) "
                                "ON CONFLICT(source, table_name, event_id) DO UPDATE SET "
                                "occurrences=occurrences+1",
                                (source, table_name, str(event_id), key),
                            )
                connection.commit()
            token = response.get("NextContinuationToken")
            if not token:
                break
        duplicate_ids, duplicate_rows = connection.execute(
            "SELECT COUNT(*), COALESCE(SUM(occurrences-1), 0) FROM events "
            "WHERE source=? AND table_name=? AND occurrences>1",
            (source, table_name),
        ).fetchone()
    finally:
        connection.close()
    report = {
        "source": source, "table": table_name, "s3_prefix": root,
        "objects_scanned": objects, "identified_rows": rows,
        "legacy_rows_without_event_id": without_id,
        "duplicate_event_ids": duplicate_ids, "duplicate_rows": duplicate_rows,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "manifest": str(database),
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = directory / f"{source}-{table_name}-{stamp}.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Report saved to {report_path}")
    return duplicate_ids == 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args()
    if not reconcile(args.source, args.table):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
