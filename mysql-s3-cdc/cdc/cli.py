"""Container-side administrative CLI."""

import argparse

from .config import load_databases, load_tables, validate_all
from .connectors import apply, statuses
from .discovery import discover


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
    subparsers.add_parser("status")
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
        apply(wait=args.wait)
    elif args.command == "status":
        statuses()


if __name__ == "__main__":
    main()

