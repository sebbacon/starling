import json
import os

from django.core.management.base import BaseCommand, CommandError

from starling_spaces import reporting, salary_automation


class Command(BaseCommand):
    help = "Run daily salary allocation and drawdown automation."

    def add_arguments(self, parser):
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
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Plan and log transfers without executing them.",
        )

    def handle(self, *args, **options):
        token = os.getenv("STARLING_PAT")
        if token is None or not token.strip():
            raise CommandError("STARLING_PAT must be set in the environment or .env file")

        try:
            summary = salary_automation.run_salary_automation(
                token.strip(),
                base_url=options["base_url"],
                timeout=options["timeout"],
                dry_run=options["dry_run"],
            )
        except (
            reporting.StarlingAPIError,
            reporting.StarlingSchemaError,
            salary_automation.SalaryAutomationError,
        ) as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(json.dumps(summary, indent=2))
