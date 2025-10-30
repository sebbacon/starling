import os
import sqlite3
from pathlib import Path

import pytest


BASE_DIR = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "starling_web.starling_web.settings")
os.environ.setdefault("DJANGO_SECRET_KEY", "test-secret-key")
os.environ.setdefault("DJANGO_DEBUG", "1")
os.environ.setdefault("DJANGO_ALLOWED_HOSTS", "localhost,testserver")
os.environ.setdefault("DJANGO_DATABASE_PATH", str(BASE_DIR / "test_django.sqlite3"))
os.environ.setdefault("STARLING_FEEDS_DB", str(BASE_DIR / "data" / "starling_feeds.db"))
os.environ.setdefault("STARLING_SUMMARY_DAYS", "30")


def pytest_configure():
    import django

    django.setup()


@pytest.fixture
def sample_feed_database(tmp_path):
    db_path = tmp_path / "feeds.db"
    _initialise_feed_database(db_path)
    return db_path


def _initialise_feed_database(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE categories (account_uid TEXT, category_type TEXT, category_uid TEXT, space_uid TEXT, name TEXT)"
        )
        conn.execute(
            "CREATE TABLE feed_items (feed_item_uid TEXT, account_uid TEXT, category_uid TEXT, space_uid TEXT, direction TEXT, amount_minor_units INTEGER, currency TEXT, transaction_time TEXT, source TEXT, counterparty TEXT, spending_category TEXT, raw_json TEXT)"
        )
        conn.execute(
            "CREATE TABLE sync_state (account_uid TEXT, category_uid TEXT, last_transaction_time TEXT)"
        )
        conn.execute(
            "INSERT INTO categories VALUES ('acc-1', 'space', 'space-1', 'space-1', 'Space One')"
        )
        conn.execute(
            "INSERT INTO categories VALUES ('acc-1', 'spending', 'SHOPPING', NULL, 'Shopping')"
        )
        conn.execute(
            "INSERT INTO feed_items VALUES ('feed-1', 'acc-1', 'space-1', 'space-1', 'OUT', -2500, 'GBP', '2024-11-10T12:00:00+00:00', NULL, 'Merchant', 'SHOPPING', '{}')"
        )
        conn.commit()
    finally:
        conn.close()
