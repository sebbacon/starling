import json
import os
from datetime import datetime, timezone

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from starling_spaces import ingestion, reporting


class Command(BaseCommand):
    help = "Calculate the average spend for spaces and spending categories."

    def add_arguments(self, parser):
        parser.add_argument(
            "--days",
            type=int,
            default=settings.STARLING_SUMMARY_DAYS,
            help="Averaging window in days.",
        )
        parser.add_argument(
            "--reference-time",
            help="Reference timestamp in ISO8601 format.",
        )
        parser.add_argument(
            "--base-url",
            default=reporting.API_BASE_URL,
            help="Starling API base URL.",
        )
        parser.add_argument(
            "--timeout",
            type=float,
            default=10.0,
            help="HTTP timeout for Starling API requests.",
        )

    def handle(self, *args, **options):
        summary = ingestion.calculate_average_spend(
            days=options["days"],
            reference_time=self._parse_reference_time(options.get("reference_time")),
        )

        token = os.getenv("STARLING_PAT")
        if token and summary.get("spaces"):
            account_uids = {item["accountUid"] for item in summary["spaces"]}
            account_uids.update(
                item["accountUid"] for item in summary.get("spendingCategories", []) if item.get("accountUid")
            )
            if account_uids:
                try:
                    balances = ingestion.fetch_account_balances(
                        token.strip(),
                        sorted(account_uids),
                        base_url=options["base_url"],
                        timeout=options["timeout"],
                    )
                except reporting.StarlingAPIError as exc:
                    summary.setdefault("errors", []).append(
                        f"Failed to fetch account balances: {exc}"
                    )
                else:
                    summary["accountBalances"] = balances

        self.stdout.write(json.dumps(summary, indent=2))

    def _parse_reference_time(self, value):
        if not value:
            return None
        cleaned = value.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(cleaned)
        except ValueError as exc:
            raise CommandError(f"Invalid reference time: {value}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
