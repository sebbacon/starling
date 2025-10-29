from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable, Optional, Sequence

from dotenv import load_dotenv

from .reporting import (API_BASE_URL, AccountReport, StarlingAPIError,
                        StarlingSchemaError, fetch_spaces_configuration,
                        iter_report_lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print configuration and balances of Starling Spaces."
    )
    parser.add_argument(
        "--account",
        "-a",
        action="append",
        help="Limit output to specific account UID (can be provided multiple times).",
    )
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
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    load_dotenv()
    token = _get_token()
    if not token:
        print(
            "STARLING_PAT is not set in environment or .env file.",
            file=sys.stderr,
        )
        return 1

    try:
        reports = fetch_spaces_configuration(
            token,
            base_url=args.base_url,
            timeout=args.timeout,
        )
    except StarlingAPIError as exc:
        print(f"Failed to fetch Spaces: {exc}", file=sys.stderr)
        return 2
    except StarlingSchemaError as exc:
        print(f"Unexpected response schema: {exc}", file=sys.stderr)
        return 3

    if args.account:
        reports = list(_filter_reports(reports, args.account))

    for line in iter_report_lines(reports):
        print(line)

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


if __name__ == "__main__":
    raise SystemExit(main())
