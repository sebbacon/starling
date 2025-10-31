import os
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "starling_web.starling_web.settings")
os.environ.setdefault("DJANGO_SECRET_KEY", "test-secret-key")
os.environ.setdefault("DJANGO_DEBUG", "1")
os.environ.setdefault("DJANGO_ALLOWED_HOSTS", "localhost,testserver")
os.environ.setdefault("DJANGO_DATABASE_PATH", str(BASE_DIR / "test_django.sqlite3"))
os.environ.setdefault("STARLING_SUMMARY_DAYS", "30")

import django
django.setup()

import pytest

from starling_web.spaces.models import Category, FeedItem


def pytest_configure():
    django.setup()


@pytest.fixture
def sample_feed_database(db):
    Category.objects.create(
        account_uid="acc-1",
        category_type="space",
        category_uid="space-1",
        space_uid="space-1",
        name="Space One",
    )
    Category.objects.create(
        account_uid="acc-1",
        category_type="spending",
        category_uid="SHOPPING",
        name="Shopping",
    )
    FeedItem.objects.create(
        feed_item_uid="feed-1",
        account_uid="acc-1",
        category_uid="space-1",
        space_uid="space-1",
        direction="OUT",
        amount_minor_units=-2500,
        currency="GBP",
        transaction_time=datetime(2024, 11, 10, 12, 0, tzinfo=timezone.utc),
        source=None,
        counterparty="Merchant",
        spending_category="SHOPPING",
        classified_category="Shopping",
        classification_reason="starling_fallback",
        raw_json={},
    )
