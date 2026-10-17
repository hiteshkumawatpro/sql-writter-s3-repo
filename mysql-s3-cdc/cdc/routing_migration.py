"""Copy legacy replacement-path objects into configured logical table paths."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError

from .config import load_s3
from .operations import table_config
from .runtime_state import RUNTIME, active_replacements, update_control_entry
from .sink import S3Writer


def migrate(source: str, table_name: str, connector: str | None, execute: bool) -> None:
    _db, table = table_config(source, table_name)
    replacements = [item for item in active_replacements() if item.get("source") == source]
    if connector is None:
        if len(replacements) != 1:
            raise ValueError(
                f"Specify --connector because {len(replacements)} active replacements were found"
            )
        connector = str(replacements[0]["connector"])
    s3 = load_s3()["s3"]
    bucket = str(s3["bucket"])
    base = str(s3.get("prefix", "")).strip("/")
    source_root = "/".join(
        part for part in (base, "replacements", connector, table.key.strip("/")) if part
    ) + "/"
    destination_root = "/".join(
        part for part in (base, table.key.strip("/")) if part
    ) + "/"
    client = S3Writer(s3).client
    token = None
    planned: list[dict[str, object]] = []
    connector_token = hashlib.sha256(connector.encode()).hexdigest()[:12]
    while True:
        arguments = {"Bucket": bucket, "Prefix": source_root}
        if token:
            arguments["ContinuationToken"] = token
        response = client.list_objects_v2(**arguments)
        for item in response.get("Contents", []):
            source_key = str(item["Key"])
            relative = source_key[len(source_root):]
            path = Path(relative)
            destination_name = f"legacy-t{connector_token}-{path.name}"
            destination_key = destination_root + str(path.with_name(destination_name)).replace("\\", "/")
            planned.append({
                "source": source_key, "destination": destination_key,
                "size": int(item["Size"]),
            })
        token = response.get("NextContinuationToken")
        if not token:
            break
    if not planned:
        raise RuntimeError(f"No legacy objects found below s3://{bucket}/{source_root}")
    print(
        f"{'Migrating' if execute else 'Would migrate'} {len(planned)} object(s) "
        f"from s3://{bucket}/{source_root} to configured path s3://{bucket}/{destination_root}"
    )
    copied = 0
    if execute:
        for item in planned:
            destination = str(item["destination"])
            expected_size = int(item["size"])
            try:
                existing = client.head_object(Bucket=bucket, Key=destination)
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
                    raise
            else:
                if int(existing["ContentLength"]) != expected_size:
                    raise RuntimeError(f"Destination collision with different size: s3://{bucket}/{destination}")
                copied += 1
                continue
            client.copy_object(
                Bucket=bucket, Key=destination,
                CopySource={"Bucket": bucket, "Key": str(item["source"])},
                MetadataDirective="COPY",
            )
            verified = client.head_object(Bucket=bucket, Key=destination)
            if int(verified["ContentLength"]) != expected_size:
                raise RuntimeError(f"Copy verification failed: s3://{bucket}/{destination}")
            copied += 1
    report = {
        "source": source, "table": table_name, "connector": connector,
        "bucket": bucket, "source_prefix": source_root,
        "destination_prefix": destination_root, "objects": planned,
        "executed": execute, "verified_objects": copied,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_objects_deleted": False,
    }
    directory = RUNTIME / "routing-migrations"
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = directory / f"{source}-{table_name}-{stamp}.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if execute and any(str(item.get("connector")) == connector for item in replacements):
        update_control_entry(
            "replacements", connector,
            logical_routing_migrated_at=report["created_at"],
        )
    print(f"Migration report: {report_path}")
    if not execute:
        print("Dry run only. Rerun with --yes after reviewing the source and destination paths.")
    else:
        print("Copy and size verification complete. Legacy source objects were preserved.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--connector")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()
    migrate(args.source, args.table, args.connector, args.yes)


if __name__ == "__main__":
    main()
