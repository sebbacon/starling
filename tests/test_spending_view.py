import json
import sqlite3
from datetime import datetime

import pytest
from django.test import Client
from django.urls import reverse

from starling_spaces.analytics import calculate_spend_by_category


pytestmark = pytest.mark.django_db


@pytest.fixture
def spend_db(tmp_path):
    db_path = tmp_path / "spend.db"
    conn = sqlite3.connect(db_path)
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
        conn.executemany(
            "INSERT INTO categories VALUES (?, ?, ?, ?, ?)",
            [
                ("acc-1", "space", "space-1", "space-1", "Space One"),
                ("acc-1", "space", "space-2", "space-2", "Space Two"),
                ("acc-1", "spending", "SHOPPING", None, "Shopping"),
            ],
        )
        conn.executemany(
            "INSERT INTO feed_items VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "item-space",
                    "acc-1",
                    "space-1",
                    "space-1",
                    "OUT",
                    -5000,
                    "GBP",
                    "2024-11-10T10:00:00+00:00",
                    "CARD",
                    "Merchant",
                    "SHOPPING",
                    "{}",
                ),
                (
                    "item-transfer",
                    "acc-1",
                    "space-1",
                    "space-1",
                    "OUT",
                    -2000,
                    "GBP",
                    "2024-11-11T09:00:00+00:00",
                    "SAVINGS_GOAL",
                    "",
                    None,
                    "{}",
                ),
                (
                    "item-main",
                    "acc-1",
                    "cat-1",
                    "",
                    "OUT",
                    -3000,
                    "GBP",
                    "2024-11-10T12:00:00+00:00",
                    "CARD",
                    "Grocer",
                    "SHOPPING",
                    "{}",
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_spending_page_renders(spend_db, settings):
    settings.STARLING_FEEDS_DB = str(spend_db)
    client = Client()
    response = client.get(reverse("spaces:spending"))
    assert response.status_code == 200
    assert "Spending Overview" in response.content.decode()
    assert "href=\"/\"" in response.content.decode()


def test_spending_data_prefers_space_category(spend_db, settings):
    settings.STARLING_FEEDS_DB = str(spend_db)
    client = Client()
    response = client.get(
        reverse("spaces:spending-data"),
        {
            "reference": "2024-11-15T00:00:00Z",
            "days": 10,
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["dates"] == ["2024-11-10"]

    space_series = next(item for item in payload["series"] if item["category"] == "Space One")
    shopping_series = next(item for item in payload["series"] if item["category"] == "Shopping")

    assert space_series["values"] == [50.0]
    assert space_series["minorValues"] == [5000]

    assert shopping_series["values"] == [30.0]
    assert shopping_series["minorValues"] == [3000]

    categories = {item["category"] for item in payload["series"]}
    assert "Space Two" not in categories

    # ensure transfer excluded
    totals = {item["category"]: item["totalMinorUnits"] for item in payload["series"]}
    assert totals["Space One"] == 5000


def test_spending_data_rejects_invalid_days(spend_db, settings):
    settings.STARLING_FEEDS_DB = str(spend_db)
    client = Client()
    response = client.get(reverse("spaces:spending-data"), {"days": 0})
    assert response.status_code == 400


def test_spending_data_defaults_to_settings_window(spend_db, settings):
    settings.STARLING_FEEDS_DB = str(spend_db)
    settings.STARLING_SUMMARY_DAYS = 1500
    client = Client()
    response = client.get(reverse("spaces:spending-data"))
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["series"]


def test_spending_data_rejects_invalid_reference(spend_db, settings):
    settings.STARLING_FEEDS_DB = str(spend_db)
    client = Client()
    response = client.get(reverse("spaces:spending-data"), {"reference": "not-a-date"})
    assert response.status_code == 400


def test_spending_data_handles_naive_reference(spend_db, settings):
    settings.STARLING_FEEDS_DB = str(spend_db)
    client = Client()
    response = client.get(
        reverse("spaces:spending-data"),
        {
            "reference": "2024-11-15T00:00:00",
            "days": 10,
        },
    )
    assert response.status_code == 200


def test_spending_data_rejects_non_numeric_days(spend_db, settings):
    settings.STARLING_FEEDS_DB = str(spend_db)
    client = Client()
    response = client.get(reverse("spaces:spending-data"), {"days": "many"})
    assert response.status_code == 400


def test_calculate_spend_by_category_validates_args(spend_db):
    with pytest.raises(ValueError):
        calculate_spend_by_category(db_path=spend_db, days=0)


def test_calculate_spend_normalises_reference(spend_db):
    summary = calculate_spend_by_category(
        db_path=spend_db,
        days=10,
        reference_time=datetime(2024, 11, 15, 12, 0, 0),
    )
    assert summary["reference"].endswith("+00:00")
