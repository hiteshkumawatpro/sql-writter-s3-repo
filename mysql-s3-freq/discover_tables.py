"""Generate table_config.yml by inspecting multiple configured MySQL databases."""

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Any

from exporter import connect, load_yaml, save_yaml, table_config_lock


INTEGER_TYPES = {
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "integer",
    "bigint",
}

DATE_TYPES = {
    "date",
    "datetime",
    "timestamp",
}


def get_database_configs(config: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Return database configurations.

    Supports both:

    databases:
      - name: db_1
        ...

    and the older single-database format:

    database:
      ...
    """
    if "databases" in config:
        databases = config["databases"]

        if not isinstance(databases, list):
            raise ValueError("'databases' must be a list.")

        if not databases:
            raise ValueError("'databases' cannot be empty.")

        return databases

    if "database" in config:
        return [config["database"]]

    raise ValueError(
        "Configuration must contain either 'database' or 'databases'."
    )


def get_connection_config(
    database_config: dict[str, Any],
) -> dict[str, Any]:
    """
    Remove discovery-only properties before passing the configuration
    to exporter.connect().
    """
    discovery_only_keys = {
        "name",
        "key_prefix",
        "enabled",
    }

    return {
        key: value
        for key, value in database_config.items()
        if key not in discovery_only_keys
    }


def build_table_key(
    global_prefix: str,
    database_prefix: str,
    source_name: str,
    table_name: str,
) -> str:
    """
    Build a unique key.

    Database-specific key_prefix takes precedence over the source name.

    Examples:
        fmea/machines
        mysql/fmea/machines
        plant_1/production
    """
    parts = []

    if global_prefix:
        parts.append(global_prefix.strip("/"))

    if database_prefix:
        parts.append(database_prefix.strip("/"))
    else:
        parts.append(source_name.strip("/"))

    parts.append(table_name)

    return "/".join(part for part in parts if part)


def discover_columns(
    database_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Fetch all base-table columns from one MySQL database."""
    schema_name = database_config["database"]

    connection = connect(database_config)
    cursor = connection.cursor(dictionary=True)

    try:
        cursor.execute(
            """
            SELECT
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.COLUMN_KEY,
                c.ORDINAL_POSITION
            FROM information_schema.COLUMNS c
            INNER JOIN information_schema.TABLES t
                ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND t.TABLE_NAME = c.TABLE_NAME
            WHERE c.TABLE_SCHEMA = %s
              AND t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY
                c.TABLE_NAME,
                c.ORDINAL_POSITION
            """,
            (schema_name,),
        )

        return cursor.fetchall()

    finally:
        cursor.close()
        connection.close()


def build_table_entries(
    columns: list[dict[str, Any]],
    source_name: str,
    schema_name: str,
    global_prefix: str,
    database_prefix: str,
    starting_order: int,
) -> list[dict[str, Any]]:
    """Convert information_schema column metadata into table configuration."""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for column in columns:
        grouped[column["TABLE_NAME"]].append(column)

    tables = []

    for name, table_columns in grouped.items():
        primary_columns = [
            column
            for column in table_columns
            if column["COLUMN_KEY"] == "PRI"
        ]

        unique_column = next(
            (
                column
                for column in table_columns
                if column["COLUMN_KEY"] == "UNI"
            ),
            None,
        )

        # A single-column primary key or unique key is considered safe
        # for incremental offset-based reading.
        safe_offset = (
            primary_columns[0]
            if len(primary_columns) == 1
            else unique_column
        )

        # If no safe unique offset exists, suggest the first integer column.
        suggested_offset = safe_offset or next(
            (
                column
                for column in table_columns
                if column["DATA_TYPE"].lower() in INTEGER_TYPES
            ),
            None,
        )

        store_column = next(
            (
                column
                for column in table_columns
                if column["DATA_TYPE"].lower() in DATE_TYPES
            ),
            None,
        )

        tables.append(
            {
                "order": starting_order + len(tables),
                "source": source_name,
                "database": schema_name,
                "table": name,
                "key": build_table_key(
                    global_prefix=global_prefix,
                    database_prefix=database_prefix,
                    source_name=source_name,
                    table_name=name,
                ),
                "store_key": (
                    store_column["COLUMN_NAME"]
                    if store_column
                    else None
                ),
                "offset_column": (
                    suggested_offset["COLUMN_NAME"]
                    if suggested_offset
                    else None
                ),
                "offset_value": None,
                "last_read": None,
                # Newly discovered tables must be reviewed before ingestion.
                # In particular, a unique column is not necessarily an
                # increasing column suitable for offset-based reads.
                "enabled": False,
            }
        )

    return tables


def write_output(
    output_path: Path,
    document: dict[str, Any],
) -> Path:
    """Write the merged table configuration."""

    try:
        save_yaml(str(output_path), document)

        return output_path

    except PermissionError:
        fallback = output_path.with_name(
            f"{output_path.stem}.generated{output_path.suffix}"
        )

        save_yaml(str(fallback), document)

        print(
            f"Could not replace {output_path}; "
            f"wrote a new file instead."
        )

        return fallback


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover tables from multiple MySQL databases."
    )

    parser.add_argument(
        "--db",
        default="db.yml",
        help="Path to the YAML configuration file.",
    )

    parser.add_argument(
        "--output",
        default="table_config.yml",
        help="Generated table configuration file.",
    )

    parser.add_argument(
        "--key-prefix",
        default="",
        help="Optional prefix applied to all generated keys.",
    )

    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue inspecting other databases when one connection fails.",
    )

    args = parser.parse_args()

    output_path = Path(args.output)

    if output_path.exists():
        table_document = load_yaml(
            str(output_path),
            expand_environment=False,
        )
        existing_tables = table_document.get("tables")

        if not isinstance(existing_tables, list):
            raise ValueError(
                f"{output_path} must contain a 'tables' list."
            )
    else:
        table_document = {"tables": []}
        existing_tables = table_document["tables"]

    for index, table in enumerate(existing_tables, start=1):
        if not isinstance(table, dict):
            raise ValueError(
                f"Existing table entry #{index} must be a YAML object."
            )

    def table_identity(table: dict[str, Any]) -> tuple[str, str, str]:
        """Identify a source table independently of mutable S3 settings."""
        return (
            str(table.get("source") or ""),
            str(table.get("database") or ""),
            str(table.get("table") or ""),
        )

    existing_identities = {
        table_identity(table)
        for table in existing_tables
    }

    orders = [
        int(table["order"])
        for table in existing_tables
        if table.get("order") is not None
    ]
    next_order = max(orders, default=0) + 1

    config = load_yaml(
        args.db,
        expand_environment=False,
    )

    database_configs = get_database_configs(config)

    new_tables: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    source_names: set[str] = set()

    for raw_database_config in database_configs:
        if not raw_database_config.get("enabled", True):
            continue

        connection_config = get_connection_config(
            raw_database_config
        )

        schema_name = connection_config.get("database")

        if not schema_name:
            raise ValueError(
                "Every database configuration must contain "
                "a 'database' value."
            )

        source_name = str(
            raw_database_config.get("name") or schema_name
        )

        if source_name in source_names:
            raise ValueError(
                f"Duplicate database source name: {source_name!r}. "
                "Give every database entry a unique 'name'."
            )

        source_names.add(source_name)

        host = connection_config.get("host", "unknown")
        port = connection_config.get("port", 3306)

        # Do not print the complete configuration because it contains
        # passwords and other sensitive values.
        print(
            f"Inspecting {source_name}: "
            f"{host}:{port}/{schema_name}"
        )

        try:
            columns = discover_columns(connection_config)

            discovered_tables = build_table_entries(
                columns=columns,
                source_name=source_name,
                schema_name=schema_name,
                global_prefix=args.key_prefix.strip("/"),
                database_prefix=str(
                    raw_database_config.get("key_prefix", "")
                ).strip("/"),
                starting_order=next_order,
            )

            added_for_source = 0

            for table in discovered_tables:
                identity = table_identity(table)

                if identity in existing_identities:
                    continue

                table["order"] = next_order
                next_order += 1
                new_tables.append(table)
                existing_identities.add(identity)
                added_for_source += 1

            print(
                f"Found {len(discovered_tables)} tables in {source_name}; "
                f"{added_for_source} are new."
            )

        except Exception as exc:
            failures.append(
                {
                    "source": source_name,
                    "error": str(exc),
                }
            )

            print(
                f"Failed to inspect {source_name}: {exc}"
            )

            if not args.continue_on_error:
                raise

    if new_tables:
        with table_config_lock(output_path):
            if output_path.exists():
                latest_document = load_yaml(
                    str(output_path),
                    expand_environment=False,
                )
                latest_tables = latest_document.get("tables")

                if not isinstance(latest_tables, list):
                    raise ValueError(
                        f"{output_path} must contain a 'tables' list."
                    )
            else:
                latest_document = {"tables": []}
                latest_tables = latest_document["tables"]

            latest_identities = {
                table_identity(table)
                for table in latest_tables
                if isinstance(table, dict)
            }
            pending_tables = [
                table
                for table in new_tables
                if table_identity(table) not in latest_identities
            ]
            latest_orders = [
                int(table["order"])
                for table in latest_tables
                if isinstance(table, dict)
                and table.get("order") is not None
            ]
            append_order = max(latest_orders, default=0) + 1

            for table in pending_tables:
                table["order"] = append_order
                append_order += 1

            latest_tables.extend(pending_tables)

            if pending_tables:
                output = write_output(
                    output_path,
                    latest_document,
                )
            else:
                output = output_path

        print(
            f"Appended {len(pending_tables)} new table(s) to {output}."
        )
    else:
        output = output_path
        print(
            f"No new tables found; {output} was left unchanged."
        )

    print(
        f"Discovery completed for "
        f"{len(source_names) - len(failures)} database(s)."
    )

    if failures:
        print(
            f"{len(failures)} database connection(s) failed."
        )

    print(
        "Review store_key and offset_column before "
        "enabling new tables."
    )


if __name__ == "__main__":
    main()
