from django.core.management.base import BaseCommand

from starling_web.spaces.models import ApplicationUser, FeedItem


class Command(BaseCommand):
    help = "Backfill the spender field on existing FeedItems from ApplicationUser mappings."

    def handle(self, *args, **options):
        user_map = {u.user_uid: u.name for u in ApplicationUser.objects.all()}
        updated = 0
        for item in FeedItem.objects.all().iterator():
            uid = item.raw_json.get("transactingApplicationUserUid")
            spender = user_map.get(uid) if uid else None
            if item.spender != spender:
                item.spender = spender
                item.save(update_fields=["spender"])
                updated += 1
        self.stdout.write(self.style.SUCCESS(f"Updated {updated} feed item(s)."))
