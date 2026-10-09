# MySQL CDC to S3 — native and offline

A standalone, Docker-free CDC stack for Windows x64. Debezium reads MySQL row-level binlog events into the repository-local Kafka installation; the Python sink filters operations per table and writes Snappy-compressed Parquet to S3.

```text
MySQL binlog -> Debezium / Kafka Connect -> Kafka -> CDC sink -> S3 Parquet
```

After this repository has been copied to a machine, normal setup and operation require no internet connection. The only external runtime prerequisite is 64-bit Python 3.11; Java 21, Kafka, Debezium, and all Python packages are stored inside this repository.

## Included offline software

- Apache Kafka 4.1.2 in `vendor/kafka/`.
- Debezium MySQL connector 3.5.2.Final in `vendor/plugins/`.
- Eclipse Temurin JRE 21 in `vendor/java/`.
- Eleven pinned Python wheels in `vendor/python-wheels/`.
- Original archives and publisher checksum files in `vendor/archives/`.

See [vendor/OFFLINE-BUNDLE.md](vendor/OFFLINE-BUNDLE.md) for versions, sources, and verified hashes. `python manage.py verify-bundle` rechecks the Kafka archive.

There are no Docker files or Docker runtime dependencies.

## 1. Prepare MySQL

MySQL must use row-based binary logging with full row images:

```ini
[mysqld]
server-id=1
log_bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
binlog_expire_logs_seconds=604800
```

Restart MySQL after applying these settings. The DBA-provided CDC user needs `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, and `REPLICATION CLIENT`; environments where a global read lock is unavailable can also require `LOCK TABLES`. Keep binlogs longer than the maximum expected pipeline outage.

## 2. Configure credentials and sources

In PowerShell:

```powershell
Copy-Item .env.example .env
```

Put MySQL passwords directly in [config/databases.yml](config/databases.yml). Use `.env` only for AWS credentials; `.env` is excluded by `.gitignore`. Configure:

- [config/databases.yml](config/databases.yml): one block per database/schema, including its literal `password`. Every enabled source needs a unique `name`, `topic_prefix`, and MySQL replication `server_id`.
- [config/s3.yml](config/s3.yml): bucket, prefix, region, optional endpoint, credentials, batching, and flush interval.
- [config/tables.yml](config/tables.yml): generated table-level capture and operation settings.

Because `databases.yml` now contains plaintext passwords, do not commit or distribute it without removing the secrets. The S3 `credentials` section can be removed to use the normal AWS SDK credential chain instead. Kafka Connect also stores resolved database credentials in its local internal Kafka topic, so protect the repository's `runtime/` directory.

## 3. Offline setup and validation

The setup command creates `.venv` and installs packages only from `vendor/python-wheels` using pip `--no-index`:

```powershell
python manage.py verify-bundle
python manage.py setup
```

## 4. Discover and enable tables

Discovery connects to every enabled MySQL database and appends only new tables. Existing entries and settings are preserved; new tables default to `enabled: false`.

```powershell
python manage.py discover
```

Example table entry:

```yaml
tables:
  - order: 1
    source: mysql_main
    database: application_db
    table: orders
    key: mysql_main/application_db/orders
    partition_column: created_at
    enabled: true
    insert: true
    update: true
    delete: false
```

- `enabled` controls Debezium capture and sink subscription.
- `insert` includes inserts and initial-snapshot rows.
- `update` includes update events.
- `delete` includes delete events using the `before` image.
- `partition_column` optionally selects a row date/timestamp; `null` uses event time in UTC.

After changing table enablement, run `python manage.py sync`. Operation switches and sink subscriptions hot-reload, but connector capture membership requires `sync`. Enabling a table after the connector's initial snapshot captures future changes; request a Debezium incremental snapshot if historical rows are also needed.

## 5. Start and operate

One entry point controls the complete native stack:

```powershell
python manage.py start
python manage.py status
python manage.py sync
python manage.py restart
python manage.py stop
```

`start` performs these actions in order:

1. Builds the offline Python environment if needed and validates YAML.
2. Formats local KRaft storage on first use.
3. Starts Kafka on `127.0.0.1:9092`.
4. Starts Kafka Connect/Debezium on `127.0.0.1:8083`.
5. Applies the configured MySQL connectors.
6. Starts the S3 sink.

Logs are written to `runtime/logs/kafka.log`, `connect.log`, and `sink.log`. PIDs are tracked in `runtime/pids/`. `stop` preserves Kafka records, connector offsets, and schema history under `runtime/kafka-data/`.

## S3 output and ordering

```text
s3://BUCKET/PREFIX/<table key>/year=YYYY/month=MM/day=DD/
  part-p00000-o00000000000000000100-00000000000000000199.parquet
```

Delivery is at least once. Kafka offsets commit only after successful S3 uploads; deterministic object names make retries overwrite the same object.

Every row contains the source columns plus `lsn`, `_cdc_operation`, source/table identity, event timestamp, binlog file/position/row, GTID, transaction ID, and Kafka topic/partition/offset. `lsn` is a zero-padded `binlog-file-number:position:row`, sortable within one MySQL source/table. It is not a global clock across independent MySQL servers.

## Deployment note

This is a single-node standalone stack. For high availability or multiple hosts, use replicated Kafka/Connect infrastructure with authentication and TLS. The Debezium setup and MySQL privileges follow the [official MySQL connector documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html). Kafka 4.1.2 and Debezium 3.5.2 are a tested compatible pair.

