"""Container-side administrative CLI."""

import argparse

from .config import load_databases, load_tables, validate_all
from .connectors import apply, statuses
from .discovery import discover


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate")
    subparsers.add_parser("discover")
    apply_parser = subparsers.add_parser("apply")
    apply_parser.add_argument("--wait", action="store_true")
    subparsers.add_parser("status")
    args = parser.parse_args()
    if args.command == "validate":
        validate_all()
        print(f"Configuration is valid: {len(load_databases())} source(s), {len(load_tables())} table(s).")
    elif args.command == "discover":
        discover()
    elif args.command == "apply":
        validate_all()
        apply(wait=args.wait)
    elif args.command == "status":
        statuses()


if __name__ == "__main__":
    main()

