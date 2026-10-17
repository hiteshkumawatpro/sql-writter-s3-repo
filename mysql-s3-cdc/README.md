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

After changing table enablement, run `python manage.py sync`. For a table newly
added to an existing healthy connector, `sync` updates the capture list and
automatically requests a Debezium blocking snapshot for only the new table.
That source pauses while existing rows are emitted, then resumes from its binlog
position; Kafka, the S3 sink, and other sources remain online. Tables without a
primary key are supported by this snapshot mode.

The connector retains schema history for visible tables before they are
enabled. This prevents future late-enabled tables from failing with `schema
isn't known`. It is preventive, not retroactive: a connector whose history was
created using the old restricted setting needs a guarded schema-history recovery
before the affected table can be safely onboarded. Never delete connector
offsets or schema-history topics while the pipeline is running.

For an already-broken connector, first stop changing that source and preserve
`runtime/kafka-data`. A Debezium `snapshot.mode=recovery` repair is safe only if
no table schema changed after the connector stopped at its recorded binlog
position. The repair must be done for that connector alone: stop it, back up its
config/status/offsets, rebuild its schema-history topic with recovery mode and
`schema.history.internal.store.only.captured.tables.ddl=false`, verify the task
is running, then return snapshot mode to `no_data` and run `sync`. If that
no-schema-change condition cannot be proven, do not automate recovery; use a new
connector/history topic or a DBA-approved full resnapshot. This guard avoids
silently decoding retained binlog events with the wrong table schema.

### Backfill a table enabled after the initial snapshot

For a one-time historical load, the manager can stream one enabled table from
MySQL directly into the same S3 Parquet layout without resetting Kafka or
Debezium offsets:

```bash
python manage.py backfill JK_BTP_PPDS BTP_Schedule_Floor_Monthly --yes
```

The command uses the source credentials from `databases.yml`/`.env`, and the
S3 key and partition column from `tables.yml`. It reads under a consistent
MySQL transaction and uploads batches of 5,000 rows by default. Override this
with `--batch-size`. Object names include the stable run identifier
`manual-v1`; rerunning with the same `--run-id` overwrites the same object
names instead of creating a second set of backfill files.

Backfill rows use `_cdc_operation: snapshot`, `_cdc_backfill: true`, and
`_cdc_backfill_run_id` metadata. A direct backfill does not alter connector or
consumer offsets. Run `sync` first so subsequent changes are captured through
normal CDC. Because changes can overlap the consistent backfill snapshot,
downstream consumers must retain their normal CDC deduplication/version rules.

## 5. Start and operate

One entry point controls the complete native stack:

```powershell
python manage.py start
python manage.py status
python manage.py progress --lines 20
python manage.py progress --follow
python manage.py sync
python manage.py restart
python manage.py restart-workers
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

After deploying only Python control/sink changes, use `restart-workers` to load
them without restarting Kafka, Kafka Connect, or any Debezium source connector.

## Source-scoped recovery and replacement

Use source-scoped commands during production recovery. They never reconcile,
restart, create, update, or remove an unrelated connector:

```bash
python manage.py status --source JK_BTP_PPDS
python manage.py sync --source JK_BTP_PPDS
python manage.py recovery-plan --source JK_BTP_PPDS
```

`recovery-plan` is read-only with respect to Kafka Connect, Kafka topics,
offsets and S3. It captures the current connector config, status and offsets in
`runtime/recovery/<source>/backup-*.json` with owner-only permissions where the
operating system supports them. These backups contain the resolved database
password and must be protected with the rest of `runtime/`.

### Guarded history recovery

History recovery is permitted only after a DBA establishes that today's table
schemas are compatible with every unprocessed event after the connector's
stored binlog offset. The flag is an assertion, not an automatic compatibility
test:

```bash
python manage.py recover --source JK_BTP_PPDS --strategy history --ddl-compatible --yes
python manage.py status --source JK_BTP_PPDS
# Inspect runtime/logs/connect.log and confirm recovery completed.
python manage.py recovery-finalize --source JK_BTP_PPDS --recovery-complete --yes
python manage.py snapshot --source JK_BTP_PPDS --table BTP_Schedule_Floor_Monthly --yes
python manage.py progress --follow
python manage.py verify-table --source JK_BTP_PPDS --table BTP_Schedule_Floor_Monthly
```

Recovery uses a new schema-history topic and preserves the old history topic,
connector offsets and Kafka data. `recovery-finalize` refuses to proceed unless
the task is running and then changes snapshot mode to `no_data`. The selected
recovery history topic is retained in ignored control state so later `start` or
`sync` operations do not revert it. The explicit `snapshot` command works even
when the table is already present in `table.include.list`.

If DDL compatibility cannot be established, do not pass `--ddl-compatible`.
Create an isolated replacement instead:

```bash
python manage.py recover --source JK_BTP_PPDS --strategy replacement \
  --server-id 6406 --suffix recovery1 --yes
python manage.py status --source JK_BTP_PPDS
```

The replacement has a separate connector identity, server ID, topic prefix,
offsets and history topic. Its S3 output is initially isolated under
`replacements/<connector>/...`. After validating its snapshot and live events:

```bash
python manage.py replacement-cutover --source JK_BTP_PPDS --verified --yes
```

Cutover stops but does not delete the original connector and routes subsequent
replacement events to normal table keys. Rollback preserves all state:

```bash
python manage.py replacement-rollback --source JK_BTP_PPDS --yes
```

Because a keyless table has no stable database row identity, no system can
automatically prove that two snapshots contain the same logical row. Keep the
isolated replacement snapshot until it has been reconciled and approved.

### Snapshot lifecycle, schema quarantine and duplicate reports

Snapshot requests and observed S3 rows are stored in
`runtime/control-state.json`. Lifecycle states are `requested`, `snapshotting`,
`streaming`, and `complete`. `verify-table` compares observed snapshot rows to a
current source count and requires a running task; concurrent source changes can
make the counts differ, in which case it safely remains incomplete.

Every new event contains `_cdc_event_id`, a stable hash of its Kafka
topic/partition/offset. To build an S3 manifest and report duplicates without
deleting or rewriting any object:

```bash
python manage.py reconcile --source JK_BTP_PPDS --table BTP_Schedule_Floor_Monthly
```

Reports and the SQLite event manifest are stored under
`runtime/reconciliation/`. Files produced before `_cdc_event_id` are reported
as legacy rows and require business-key or source-LSN reconciliation.

Rows that cannot share a valid Arrow/Parquet schema are isolated under
`runtime/quarantine/` with the original record and error. Their Kafka offsets
are committed only after the quarantine file is flushed to disk, allowing other
topic partitions to continue. Operators must correct and replay quarantined
records; quarantine is not a replacement for S3 archival.

Connector application is isolated per database. A MySQL source that times out
or fails Debezium validation is logged and skipped while other connectors and
the S3 sink continue starting. Run `python manage.py status` afterward because
a connector accepted by the REST API can still fail asynchronously.
The manager waits for applied tasks and retries transient `FAILED` task states
for a bounded startup period; a persistently unavailable source is logged and
does not prevent other sources from running. The watchdog retries transient
failures at most three times with backoff. It does not continuously restart
schema-history, missing-binlog, or disk-full failures. `status` prints task
failure traces, returns a failing exit code when unhealthy, and reports runtime
disk space. Startup requires at least 10 GiB and 5% free; override the GiB
threshold with `CDC_MIN_FREE_GB`.

Only sources having at least one enabled table whose `source` and `database`
exactly match `databases.yml` receive a Debezium connector. `sync` removes stale
managed connectors for renamed sources and sources with no matching enabled
tables, preventing unwanted topics and CDC reads. Change events remain limited
to enabled tables, while schema history retains DDL for visible tables in the
configured database so future onboarding is safe.

Successful upload progress is stored in ignored runtime state at
`runtime/table-progress.yml`; the sink never rewrites tracked `tables.yml`.
This keeps normal EC2 operation from creating Git changes that block `git pull`.

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

