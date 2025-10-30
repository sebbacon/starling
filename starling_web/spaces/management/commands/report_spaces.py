import json
import os

from django.core.management.base import BaseCommand, CommandError

from starling_spaces import reporting


class Command(BaseCommand):
    help = "Fetch the current Starling spaces configuration and emit it as JSON."

    def add_arguments(self, parser):
        parser.add_argument(
            "--account",
            action="append",
            dest="accounts",
            default=None,
            help="Restrict the report to specific account UIDs.",
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
        token = os.getenv("STARLING_PAT")
        if token is None or not token.strip():
            raise CommandError("STARLING_PAT must be set in the environment or .env file")

        try:
            reports = reporting.fetch_spaces_configuration(
                token.strip(),
                base_url=options["base_url"],
                timeout=options["timeout"],
            )
        except (reporting.StarlingAPIError, reporting.StarlingSchemaError) as exc:
            raise CommandError(str(exc)) from exc

        accounts = options.get("accounts")
        if accounts:
            reports = [
                report
                for report in reports
                if report.account_uid in set(accounts)
            ]

        payload = reporting.build_report_payload(reports)
        self.stdout.write(json.dumps(payload, indent=2))
