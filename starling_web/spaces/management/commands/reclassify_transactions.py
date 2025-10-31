
from django.core.management.base import BaseCommand

from starling_spaces.classification import classify_transaction, reset_rules_cache
from starling_web.spaces.models import Category, FeedItem


class Command(BaseCommand):
    help = "Re-run transaction classification rules and persist updates."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            help="Limit the number of rows to reclassify (useful for testing).",
        )

    def handle(self, *args, **options):
        reset_rules_cache()

        space_names = {
            (cat.account_uid, cat.category_uid): cat.name
            for cat in Category.objects.filter(category_type="space")
        }

        queryset = FeedItem.objects.order_by("transaction_time")
        limit = options.get("limit")
        if limit:
            queryset = queryset[:limit]

        updates = 0
        for item in queryset:
            space_name = space_names.get((item.account_uid, item.space_uid))
            classification = classify_transaction(
                {
                    "space_uid": item.space_uid,
                    "space_name": space_name,
                    "spending_category": item.spending_category,
                    "counterparty": item.counterparty,
                    "source": item.source,
                    "amount_minor_units": item.amount_minor_units,
                    "direction": item.direction,
                    "raw": item.raw_json,
                }
            )

            if (
                classification.category != item.classified_category
                or classification.reason != item.classification_reason
            ):
                item.classified_category = classification.category
                item.classification_reason = classification.reason
                item.save(update_fields=["classified_category", "classification_reason"])
                updates += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Reclassified {updates} record{'s' if updates != 1 else ''}."
            )
        )
        return 0
