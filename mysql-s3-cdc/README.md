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

See [vendor/OFFLINE-BUNDLE.md](vendor/OFFLINE-BUNDLE.md) for versions, sources,
and verified hashes. `python manage.py verify-bundle` checks the extracted
runtime and rechecks the Kafka source archive when its Git LFS payload is
available.

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

`ssl_mode` controls the Debezium-to-MySQL connection. Supported values are
`disabled`, `preferred`, `required`, `verify_ca`, and `verify_identity`. This
configuration uses `disabled` because the current private-network MySQL endpoint
resets TLS negotiation. Prefer `verify_identity` with a trusted certificate when
TLS is configured on MySQL; `disabled` sends database traffic unencrypted.

## 3. Offline setup and validation

The setup command creates `.venv` and installs packages only from `vendor/python-wheels` using pip `--no-index`:

```powershell
python manage.py verify-bundle
python manage.py setup
```

## 4. Discover and enable tables

Discovery connects to every enabled MySQL database and appends only new tables. Existing entries and settings are preserved; new tables default to `enabled: false`.

Before listing tables, discovery checks that binary logging is enabled with
`binlog_format=ROW` and `binlog_row_image=FULL`, and that the configured user
advertises the required CDC grants. A database that cannot connect or is not
CDC-ready is logged as `WARNING` and skipped; other configured databases still
continue. Existing entries for a skipped source are never deleted or changed.

```powershell
python manage.py discover
```

By default, newly discovered tables are disabled for review. To enable every
newly discovered table immediately, use:

```powershell
python manage.py discover --enable-new
```

`--enable-new` affects only tables appended by that run. Existing entries and
their current `enabled`, operation, partition, and S3-key settings are preserved.

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
python manage.py progress --lines 20
python manage.py progress --follow
python manage.py sync
python manage.py restart
python manage.py stop
```

To stop the pipeline and permanently erase the local Kafka records,
consumer/connector offsets, Debezium schema history, runtime logs, and PID files:

```powershell
python manage.py fresh-start --yes
```

The confirmation flag is mandatory. This command preserves YAML configuration,
credentials, source databases, and existing S3 objects, and leaves all services
stopped. Run `python manage.py start` later to create a new Kafka state. Because
offsets were deleted, Debezium then performs fresh snapshots and existing rows
can be archived to S3 again. Use `restart` instead when Kafka state and offsets
must be retained.

`start` performs these actions in order:

1. Builds the offline Python environment if needed and validates YAML.
2. Formats local KRaft storage on first use.
3. Starts Kafka on `127.0.0.1:9092`.
4. Starts Kafka Connect/Debezium on `127.0.0.1:8083`.
5. Applies the configured MySQL connectors.
6. Starts the S3 sink.

Logs are written under `runtime/logs/`:

- `kafka.log`: Kafka broker startup and runtime messages.
- `connect.log`: Kafka Connect and Debezium connector/task errors.
- `sink.log`: Kafka consumption and S3 upload messages.
- `watchdog.log`: automatic recovery attempts for failed Debezium tasks.
- `manager.log`: validation, connector application, status, and startup errors.

Every successful object upload is logged with its CDC row count, full S3 URI,
Kafka topic, partition, and offset range, for example:

```text
2026-10-12 06:59:07,960 | INFO | KafkaS3Archive | Uploaded 3 CDC rows to s3://bucket/prefix/table/year=2026/month=10/day=12/part-p00000-o00000000000000000100-00000000000000000102.parquet [topic=source.database.table partition=0 offsets=100-102]
```

`status` includes the five latest S3 activity lines. Use
`python manage.py progress --follow` for a live filtered view. When hosted as a
systemd service, `journalctl -u <service-name> -f` shows the same sink output if
the service sends stdout/stderr to the journal.

PIDs are tracked in `runtime/pids/`. `stop` preserves Kafka records, connector
offsets, and schema history under `runtime/kafka-data/`.

Connector application is isolated per database. A MySQL source that times out
or fails Debezium validation is logged and skipped while other connectors and
the S3 sink continue starting. Run `python manage.py status` afterward because
a connector accepted by the REST API can still fail asynchronously.
The manager waits for applied tasks and retries transient `FAILED` task states
for a bounded startup period; a persistently unavailable source is logged and
does not prevent other sources from running. While the stack is running, the
watchdog checks managed connector tasks every 30 seconds and restarts only tasks
in `FAILED` state.

Only sources having at least one enabled table whose `source` and `database`
exactly match `databases.yml` receive a Debezium connector. `sync` removes stale
managed connectors for renamed sources and sources with no matching enabled
tables, preventing unwanted topics and CDC reads. Debezium schema history is
also restricted to those captured tables; it does not scan schema metadata for
every database visible to the MySQL account during startup. After applying a
configuration, `sync` also restarts any task that was already in `FAILED` state.

During an initial snapshot, an enabled table topic might not exist immediately.
The S3 sink logs that it is waiting and keeps polling until Debezium creates the
topic; this condition does not stop the pipeline.

S3 upload failures are logged and retried after `sink.upload_retry_seconds`;
Kafka offsets are not committed and the pending batch is retained. Startup
validation rejects example bucket names, so `s3.bucket` must name an existing
bucket in the configured region.

## S3 output and ordering

```text
s3://BUCKET/PREFIX/<table key>/year=YYYY/month=MM/day=DD/
  part-p00000-o00000000000000000100-00000000000000000199.parquet
```

Delivery is at least once. Kafka offsets commit only after successful S3 uploads; deterministic object names make retries overwrite the same object.

Every row contains the source columns plus `lsn`, `_cdc_operation`, source/table identity, event timestamp, binlog file/position/row, GTID, transaction ID, and Kafka topic/partition/offset. `lsn` is a zero-padded `binlog-file-number:position:row`, sortable within one MySQL source/table. It is not a global clock across independent MySQL servers.

## Deployment note

This is a single-node standalone stack. For high availability or multiple hosts, use replicated Kafka/Connect infrastructure with authentication and TLS. The Debezium setup and MySQL privileges follow the [official MySQL connector documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html). Kafka 4.1.2 and Debezium 3.5.2 are a tested compatible pair.

