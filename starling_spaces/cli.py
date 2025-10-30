from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence

from dotenv import load_dotenv

from .ingestion import (DEFAULT_DB_PATH, calculate_average_spend,
                        sync_space_feeds)
from .reporting import (API_BASE_URL, AccountReport, StarlingAPIError,
                        StarlingSchemaError, build_report_payload,
                        fetch_spaces_configuration)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Tools for Starling Spaces."
    )
    _add_report_arguments(parser)

    subparsers = parser.add_subparsers(dest="command")

    report_parser = subparsers.add_parser("report", help="Print Spaces report as JSON.")
    _add_report_arguments(report_parser)

    ingest_parser = subparsers.add_parser(
        "ingest-feeds", help="Sync feed items for each space into SQLite storage."
    )
    _add_base_arguments(ingest_parser)
    ingest_parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database path (default: {DEFAULT_DB_PATH}).",
    )
    ingest_parser.add_argument(
        "--changes-since",
        help="Override sync cursor with ISO timestamp (optional).",
    )
    ingest_parser.add_argument(
        "--max-pages",
        type=int,
        help="Limit number of pages fetched per space (optional).",
    )

    average_parser = subparsers.add_parser(
        "average-spend",
        help="Calculate average spend per space from stored feed data.",
    )
    average_parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database path (default: {DEFAULT_DB_PATH}).",
    )
    average_parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Averaging window in days (default: 30).",
    )
    average_parser.add_argument(
        "--reference-time",
        help="Reference timestamp (ISO 8601). Defaults to now.",
    )

    parser.set_defaults(command="report")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    if argv is None:
        parsed_argv = sys.argv[1:]
    else:
        parsed_argv = list(argv)
    filtered_argv = [item for item in parsed_argv if item != "--"]
    args = parser.parse_args(filtered_argv)

    command = args.command or "report"

    if command == "average-spend":
        summary = calculate_average_spend(
            db_path=args.db,
            days=args.days,
            reference_time=_parse_reference_time(args.reference_time),
        )
        print(json.dumps(summary, indent=2))
        return 0

    load_dotenv()
    token = _get_token()
    if not token:
        print(
            "STARLING_PAT is not set in environment or .env file.",
            file=sys.stderr,
        )
        return 1

    if command == "ingest-feeds":
        try:
            sync_space_feeds(
                token,
                db_path=args.db,
                base_url=args.base_url,
                timeout=args.timeout,
                changes_since=args.changes_since,
                max_pages=args.max_pages,
            )
        except StarlingSchemaError as exc:
            return _handle_failure(exc)
        return 0

    try:
        reports = fetch_spaces_configuration(
            token,
            base_url=args.base_url,
            timeout=args.timeout,
        )
    except (StarlingAPIError, StarlingSchemaError) as exc:
        return _handle_failure(exc)

    if args.account:
        reports = list(_filter_reports(reports, args.account))

    payload = build_report_payload(reports)
    print(json.dumps(payload, indent=2))

    return 0


def _get_token() -> Optional[str]:
    token = os.getenv("STARLING_PAT")
    if not token:
        return None
    token = token.strip()
    return token or None


def _filter_reports(
    reports: Sequence[AccountReport], allowed_uids: Iterable[str]
) -> Sequence[AccountReport]:
    if not allowed_uids:
        return list(reports)
    allowed = set(allowed_uids)
    return [report for report in reports if report.account_uid in allowed]


def _parse_reference_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise SystemExit(f"Invalid reference time: {value}") from exc
    if parsed.tzinfo is None:
        from datetime import timezone

        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _add_report_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--account",
        "-a",
        action="append",
        help="Limit output to specific account UID (can be provided multiple times).",
    )
    _add_base_arguments(parser)


def _add_base_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--base-url",
        default=API_BASE_URL,
        help=f"Starling API base URL (default: {API_BASE_URL}).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="HTTP timeout in seconds (default: 10).",
    )


def _handle_failure(exc: Exception) -> int:
    if isinstance(exc, StarlingSchemaError):
        message = f"Unexpected response schema: {exc}"
        code = 3
    else:
        message = f"Failed to fetch Spaces: {exc}"
        code = 2
    print(message, file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
