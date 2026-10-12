# Hosting the frequency-based MySQL-to-S3 pipeline on Ubuntu EC2

This guide installs `mysql-s3-freq` at `/opt/mysql-s3-exporter` and runs it
continuously as the `mysql-s3-exporter` systemd service.

The pipeline polls each enabled MySQL table, reads at most 100 new rows per
cycle, writes Parquet files, uploads them to S3, and then records its checkpoint
in `table_config.yml`.

## 1. Information required before deployment

Collect the following information before configuring the server:

| Item | Required information |
| --- | --- |
| EC2 | Ubuntu instance with SSH access and enough disk space to install Python dependencies |
| MySQL network | Host/IP and port for every MySQL server; the EC2 instance must be able to reach them |
| MySQL login | Read-only username and password for every configured database |
| S3 | Bucket name and AWS region |
| AWS credentials | Access key ID and secret access key with permission to upload into the bucket |
| Tables | Source database, table name, S3 key prefix, increasing offset column, and optional date partition column |
| Repository | Git clone URL and credentials if the repository is private |

Recommended EC2/network rules:

- Allow inbound SSH (`22`) only from an administrator IP or use AWS Systems
  Manager instead of public SSH.
- Allow outbound HTTPS (`443`) to S3.
- Allow outbound MySQL traffic to each database host, normally on port `3306`.
- Configure the database security group, firewall, routing, VPN, or VPC peering
  to accept traffic from the EC2 instance.

The AWS identity needs at least `s3:PutObject` for objects in the destination
bucket. If the bucket uses a customer-managed KMS key, it also needs the
appropriate KMS encryption permissions.

Example S3 permission, with the bucket name replaced:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME/*"
    }
  ]
}
```

## 2. Prepare Ubuntu

Connect to the EC2 instance and install the operating-system packages:

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip rsync
```

Create the non-login service account. If it already exists, `useradd` will
report that and no new account is needed.

```bash
sudo useradd --system \
  --home /opt/mysql-s3-exporter \
  --shell /usr/sbin/nologin \
  mysql-exporter
```

## 3. Copy the application

Clone the repository into the Ubuntu user's home directory:

```bash
cd ~
git clone YOUR_REPOSITORY_URL sql-writter-s3-repo
cd sql-writter-s3-repo
```

Copy only the frequency-based pipeline. Do not copy a virtual environment from
another computer; Python environments are operating-system and path specific.

```bash
sudo mkdir -p /opt/mysql-s3-exporter
sudo rsync -a \
  --exclude='.venv/' \
  --exclude='__pycache__/' \
  --exclude='*.pem' \
  mysql-s3-freq/ /opt/mysql-s3-exporter/
sudo chown -R mysql-exporter:mysql-exporter /opt/mysql-s3-exporter
```

Create a fresh virtual environment and install the pinned dependencies:

```bash
sudo -u mysql-exporter python3 -m venv /opt/mysql-s3-exporter/.venv
sudo -u mysql-exporter \
  /opt/mysql-s3-exporter/.venv/bin/python -m pip install --upgrade pip
sudo -u mysql-exporter \
  /opt/mysql-s3-exporter/.venv/bin/pip install \
  -r /opt/mysql-s3-exporter/requirements.txt
```

## 4. Configure `db.yml`

Edit the production configuration:

```bash
sudo -u mysql-exporter nano /opt/mysql-s3-exporter/db.yml
```

Each entry under `databases` accepts these parameters:

| Parameter | Meaning |
| --- | --- |
| `name` | Unique source name used by `table_config.yml` |
| `driver` | Must be `mysql` |
| `host` | MySQL hostname or private IP |
| `port` | MySQL port, normally `3306` |
| `username` | MySQL user with read access |
| `password` | MySQL password |
| `database` | MySQL schema/database name |
| `enabled` | `true` to make this source available |
| `connection_timeout` | Optional connection timeout in seconds; default is `15` |
| `time_zone` | Optional MySQL session time zone |

The S3 section must keep its environment references:

```yaml
s3:
  bucket: "${S3_BUCKET}"
  access_key_id: "${AWS_ACCESS_KEY_ID}"
  secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
  prefix: ""
  region: "ap-south-1"

batch_size: 100
sleep_seconds: 60
```

`batch_size` is hard-capped at 100 rows by the exporter. `sleep_seconds` is the
delay after one pass across all enabled tables.

Because `db.yml` contains MySQL passwords, restrict access to the service user:

```bash
sudo chmod 600 /opt/mysql-s3-exporter/db.yml
```

## 5. Create the systemd secrets file

Create the environment file referenced by the service:

```bash
sudo nano /etc/mysql-s3-exporter.env
```

Enter the real production values. Do not add `export`, spaces around `=`, or
quotes unless a value requires systemd quoting.

```text
S3_BUCKET=your-production-bucket
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
```

Protect the file:

```bash
sudo chown root:root /etc/mysql-s3-exporter.env
sudo chmod 600 /etc/mysql-s3-exporter.env
```

Never commit this file or paste its real values into `db.yml`, documentation,
shell history, tickets, or chat messages.

## 6. Configure tables and checkpoints

`table_config.yml` controls which tables run. Its important fields are:

| Parameter | Meaning |
| --- | --- |
| `order` | Processing order |
| `source` | Must match a `databases[].name` value in `db.yml` |
| `database` | Expected MySQL database/schema |
| `table` | Source table name |
| `key` | Base S3 object-key prefix for the table |
| `store_key` | Date/datetime column used for year/month/day partitions, or `null` |
| `offset_column` | Unique, increasing column used for incremental reads |
| `offset_value` | Last successfully uploaded value; `null` starts from the beginning |
| `last_read` | Runtime timestamp maintained by the exporter |
| `enabled` | Only entries set to `true` are exported |

To discover tables that do not exist in `table_config.yml`:

```bash
cd /opt/mysql-s3-exporter
sudo -u mysql-exporter .venv/bin/python discover_tables.py
```

Discovery is append-only and creates new entries with `enabled: false`. Before
enabling one, confirm that:

1. `source` points to the intended database connection.
2. `offset_column` is non-null, unique, and always increasing.
3. `store_key` contains a valid date/datetime when date partitioning is used.
4. `key` is the intended S3 destination.
5. `offset_value` is correct. A `null` value begins exporting from the oldest
   rows; an existing value resumes strictly after that checkpoint.

The service updates `table_config.yml` after successful S3 uploads, so the
service user needs write permission on both the file and its directory.

Back up `table_config.yml` before replacing or editing checkpoints:

```bash
sudo cp /opt/mysql-s3-exporter/table_config.yml \
  /opt/mysql-s3-exporter/table_config.yml.backup
```

## 7. Install and start the service

Install the supplied unit file:

```bash
sudo cp /opt/mysql-s3-exporter/mysql-s3-exporter.service \
  /etc/systemd/system/mysql-s3-exporter.service
sudo systemctl daemon-reload
sudo systemctl enable mysql-s3-exporter
sudo systemctl start mysql-s3-exporter
```

Check its status and follow its logs:

```bash
sudo systemctl status mysql-s3-exporter --no-pager
sudo journalctl -u mysql-s3-exporter -f
```

A healthy log shows the number of enabled tables, fetched row counts, uploaded
S3 paths, completed tables, and the sleep interval. Let at least one full cycle
finish, then confirm that Parquet objects exist at the expected S3 keys.

The exporter catches a failure for one table and continues with the others.
Therefore, an `active (running)` service can still have table-level errors;
always inspect the journal after deployment.

## 8. Useful service commands

```bash
# Restart after changing db.yml, the environment file, Python code, or the unit
sudo systemctl restart mysql-s3-exporter

# Stop the pipeline
sudo systemctl stop mysql-s3-exporter

# Start the pipeline
sudo systemctl start mysql-s3-exporter

# Show logs from the current boot
sudo journalctl -u mysql-s3-exporter -b --no-pager

# Show the last 200 log lines
sudo journalctl -u mysql-s3-exporter -n 200 --no-pager
```

Changes to `table_config.yml` are detected automatically and normally do not
require a restart. Changes to `db.yml` or `/etc/mysql-s3-exporter.env` require a
service restart.

## 9. Updating the deployment

The runtime `offset_value` and `last_read` fields live in `table_config.yml`.
Do not overwrite this file during a routine code update or already-exported rows
could be processed again.

```bash
sudo systemctl stop mysql-s3-exporter
sudo cp /opt/mysql-s3-exporter/table_config.yml /tmp/table_config.yml.backup

cd ~/sql-writter-s3-repo
git pull
sudo rsync -a \
  --exclude='.venv/' \
  --exclude='__pycache__/' \
  --exclude='*.pem' \
  --exclude='db.yml' \
  --exclude='table_config.yml' \
  mysql-s3-freq/ /opt/mysql-s3-exporter/

sudo -u mysql-exporter \
  /opt/mysql-s3-exporter/.venv/bin/pip install \
  -r /opt/mysql-s3-exporter/requirements.txt
sudo chown -R mysql-exporter:mysql-exporter /opt/mysql-s3-exporter
sudo cp /opt/mysql-s3-exporter/mysql-s3-exporter.service \
  /etc/systemd/system/mysql-s3-exporter.service
sudo systemctl daemon-reload
sudo systemctl start mysql-s3-exporter
sudo journalctl -u mysql-s3-exporter -n 100 --no-pager
```

## 10. Common failures

### `Environment variable ... is not set`

Confirm `/etc/mysql-s3-exporter.env` exists, contains all three required values,
and the installed systemd unit has:

```ini
EnvironmentFile=/etc/mysql-s3-exporter.env
```

Run `sudo systemctl daemon-reload` and restart the service.

### `AccessDenied` during S3 upload

Check the access key, secret key, bucket name, bucket policy, IAM permissions,
AWS region, and KMS permissions when applicable.

### MySQL connection timeout or refused connection

Check the MySQL IP/hostname and port, EC2 routing, both security groups, network
ACLs, host firewall, MySQL bind address, and the database user's allowed source
host.

### Permission denied for `table_config.yml` or its lock file

Restore ownership and restart:

```bash
sudo chown -R mysql-exporter:mysql-exporter /opt/mysql-s3-exporter
sudo systemctl restart mysql-s3-exporter
```

### `status=203/EXEC`

The virtual environment or Python executable is missing. Recreate `.venv` under
`/opt/mysql-s3-exporter` and reinstall `requirements.txt`.

### `status=217/USER`

The `mysql-exporter` Linux account does not exist. Create it using the command in
section 2.

### Service runs but uploads nothing

Check the journal, confirm tables have `enabled: true`, verify their
`offset_value`, and confirm rows exist with an offset greater than the stored
checkpoint.

## 11. Operational behavior and limitations

- Progress is saved only after an S3 upload succeeds.
- A crash after upload but before checkpoint save can produce a duplicate file.
- The offset method captures new inserts. It does not reliably capture updates
  or deletes; use MySQL binlog/CDC for those requirements.
- Keep only one exporter instance active for the same `table_config.yml` and S3
  paths unless duplicate-processing behavior has been designed explicitly.
- Monitor the systemd journal and S3 delivery instead of relying only on the
  service's `active` status.
- Rotate AWS and database credentials regularly and immediately after any
  suspected exposure.
