new # MySQL to S3 Parquet

Small service with two scripts and two YAML configuration files.

```text
exporter.py                   continuous exporter with hot table-config reload
discover_tables.py           appends newly discovered tables to table_config.yml
db.yml                       MySQL, S3 and polling settings
table_config.yml             generated ordered table settings
mysql-s3-exporter.service    systemd service
```

The MySQL connection and non-secret S3 settings are configured in `db.yml`.
The S3 bucket and credentials are supplied through environment variables.
For an interactive setup, export them before running the scripts:

```bash
export S3_BUCKET='your-s3-bucket'
export AWS_ACCESS_KEY_ID='your-access-key-id'
export AWS_SECRET_ACCESS_KEY='your-secret-access-key'
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python discover_tables.py
```

Discovery is append-only: it preserves all existing entries and their runtime
checkpoints, adds only newly discovered source tables, and leaves the file
unchanged when there is nothing new. Every new table is created with
`enabled: false`. Review its `store_key` and confirm that `offset_column` is
unique and increasing before changing `enabled` to `true`.

With `store_key: created_at`, output is written as:

```text
key/year=2026/month=08/day=04/part-....parquet
```

With `store_key: null`, output stays directly under `key/`. Runtime `offset_value` and `last_read` are updated atomically in the same `table_config.yml` entry only after an S3 upload succeeds. The service user therefore needs write permission on this file and its containing directory.

Each cycle reads at most 100 new rows per enabled table. Every Parquet file therefore contains at most 100 rows. After all tables have had one batch, the service sleeps for `sleep_seconds` (60 by default) before the next cycle. If a 100-row batch spans multiple `store_key` dates, it creates one smaller Parquet file per date.

The running exporter reloads `table_config.yml` at the start of every cycle and
checks once per second for changes while it is waiting. New tables and changes
to `enabled`, ordering, partition keys, or offsets therefore take effect without
restarting the systemd service. If the file is temporarily invalid while being
edited, the exporter logs the error and uses the last valid configuration for
that cycle.

Run once or continuously:

```bash
python exporter.py --once
python exporter.py
```

For Linux, copy the repository to `/opt/mysql-s3-exporter`, create
`/etc/mysql-s3-exporter.env`, and copy `mysql-s3-exporter.service` into
`/etc/systemd/system/`.

Create `/etc/mysql-s3-exporter.env` with permissions restricted to root:

```text
S3_BUCKET=your-s3-bucket
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
```

```bash
sudo chown root:root /etc/mysql-s3-exporter.env
sudo chmod 600 /etc/mysql-s3-exporter.env
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now mysql-s3-exporter
sudo journalctl -u mysql-s3-exporter -f
```

The EC2 instance needs network access to the configured MySQL servers. The AWS
credentials in the service environment need `s3:PutObject` permission for the
configured bucket.
This offset approach handles inserts; use MySQL binlog/CDC if updates and deletes
must also be captured.
