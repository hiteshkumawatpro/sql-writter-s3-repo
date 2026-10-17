"""Container-side administrative CLI."""

import argparse
import sys

from .config import load_databases, load_tables, validate_all
from .connectors import apply, statuses
from .discovery import discover
from .operations import (
    adopt_replacement, begin_history_recovery, create_replacement,
    finalize_history_recovery,
    handoff_to_primary, recovery_plan, replacement_cutover,
    replacement_rollback, snapshot_table, verify_table,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate")
    discover_parser = subparsers.add_parser("discover")
    discover_parser.add_argument(
        "--enable-new",
        action="store_true",
        help="Enable newly discovered tables; existing entries are unchanged.",
    )
    apply_parser = subparsers.add_parser("apply")
    apply_parser.add_argument("--wait", action="store_true")
    apply_parser.add_argument("--source")
    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--source")
    plan_parser = subparsers.add_parser("recovery-plan")
    plan_parser.add_argument("--source", required=True)
    recover_parser = subparsers.add_parser("recover")
    recover_parser.add_argument("--source", required=True)
    recover_parser.add_argument("--strategy", choices=("history", "replacement"), required=True)
    recover_parser.add_argument("--ddl-compatible", action="store_true")
    recover_parser.add_argument("--missing-schema-confirmed", action="store_true")
    recover_parser.add_argument("--server-id", type=int)
    recover_parser.add_argument("--suffix")
    recover_parser.add_argument("--yes", action="store_true")
    finalize_parser = subparsers.add_parser("recovery-finalize")
    finalize_parser.add_argument("--source", required=True)
    finalize_parser.add_argument("--recovery-complete", action="store_true")
    finalize_parser.add_argument("--yes", action="store_true")
    snapshot_parser = subparsers.add_parser("snapshot")
    snapshot_parser.add_argument("--source", required=True)
    snapshot_parser.add_argument("--table", required=True)
    snapshot_parser.add_argument("--yes", action="store_true")
    verify_parser = subparsers.add_parser("verify-table")
    verify_parser.add_argument("--source", required=True)
    verify_parser.add_argument("--table", required=True)
    cutover_parser = subparsers.add_parser("replacement-cutover")
    cutover_parser.add_argument("--source", required=True)
    cutover_parser.add_argument("--verified", action="store_true")
    cutover_parser.add_argument("--yes", action="store_true")
    rollback_parser = subparsers.add_parser("replacement-rollback")
    rollback_parser.add_argument("--source", required=True)
    rollback_parser.add_argument("--yes", action="store_true")
    adopt_parser = subparsers.add_parser("adopt-replacement")
    adopt_parser.add_argument("--source", required=True)
    adopt_parser.add_argument("--connector", required=True)
    handoff_parser = subparsers.add_parser("primary-handoff")
    handoff_parser.add_argument("--source", required=True)
    handoff_parser.add_argument("--verified", action="store_true")
    handoff_parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()
    if args.command == "validate":
        warnings = validate_all()
        for warning in warnings:
            print(f"WARNING: {warning}")
        print(f"Configuration is valid: {len(load_databases())} source(s), {len(load_tables())} table(s).")
    elif args.command == "discover":
        discover(enable_new=args.enable_new)
    elif args.command == "apply":
        warnings = validate_all()
        for warning in warnings:
            print(f"WARNING: {warning}")
        apply(wait=args.wait, source=args.source)
    elif args.command == "status":
        if not statuses(source=args.source):
            sys.exit(1)
    elif args.command == "recovery-plan":
        recovery_plan(args.source)
    elif args.command == "recover":
        if not args.yes:
            raise ValueError("recover changes connector state; rerun with --yes")
        if args.strategy == "history":
            begin_history_recovery(
                args.source, args.ddl_compatible, args.missing_schema_confirmed
            )
        else:
            if args.server_id is None:
                raise ValueError("replacement recovery requires --server-id")
            create_replacement(args.source, args.server_id, args.suffix)
    elif args.command == "recovery-finalize":
        if not args.yes:
            raise ValueError("recovery-finalize changes connector configuration; rerun with --yes")
        finalize_history_recovery(args.source, args.recovery_complete)
    elif args.command == "snapshot":
        if not args.yes:
            raise ValueError("snapshot reads all historical rows; rerun with --yes")
        snapshot_table(args.source, args.table)
    elif args.command == "verify-table":
        if not verify_table(args.source, args.table):
            sys.exit(1)
    elif args.command == "replacement-cutover":
        if not args.yes:
            raise ValueError("replacement-cutover stops the original connector; rerun with --yes")
        replacement_cutover(args.source, args.verified)
    elif args.command == "replacement-rollback":
        if not args.yes:
            raise ValueError("replacement-rollback changes running connectors; rerun with --yes")
        replacement_rollback(args.source)
    elif args.command == "adopt-replacement":
        adopt_replacement(args.source, args.connector)
    elif args.command == "primary-handoff":
        if not args.yes:
            raise ValueError("primary-handoff stops the replacement connector; rerun with --yes")
        handoff_to_primary(args.source, args.verified)


if __name__ == "__main__":
    main()

