import os
from django.core.management.base import BaseCommand, CommandError

from starling_spaces import ingestion, reporting


class Command(BaseCommand):
    help = "Sync feed items for each Starling space into the Django database."

    def add_arguments(self, parser):
        parser.add_argument(
            "--changes-since",
            help="Override the sync cursor with an ISO8601 timestamp.",
        )
        parser.add_argument(
            "--max-pages",
            type=int,
            help="Limit the number of pages fetched per space.",
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
            ingestion.sync_space_feeds(
                token.strip(),
                base_url=options["base_url"],
                timeout=options["timeout"],
                changes_since=options.get("changes_since"),
                max_pages=options.get("max_pages"),
            )
        except reporting.StarlingSchemaError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS("Feed synchronisation complete."))
