import json
from datetime import datetime, timezone

import pytest
from django.test import Client
from django.urls import reverse

from starling_spaces.analytics import calculate_spend_by_category
from starling_web.spaces.models import Category, FeedItem


pytestmark = pytest.mark.django_db


def _seed_transactions():
    Category.objects.bulk_create(
        [
            Category(
                account_uid="acc-1",
                category_type="space",
                category_uid="space-1",
                space_uid="space-1",
                name="Space One",
            ),
            Category(
                account_uid="acc-1",
                category_type="space",
                category_uid="space-2",
                space_uid="space-2",
                name="Space Two",
            ),
            Category(
                account_uid="acc-1",
                category_type="spending",
                category_uid="SHOPPING",
                name="Shopping",
            ),
        ]
    )
    FeedItem.objects.bulk_create(
        [
            FeedItem(
                feed_item_uid="item-space",
                account_uid="acc-1",
                category_uid="space-1",
                space_uid="space-1",
                direction="OUT",
                amount_minor_units=-5000,
                currency="GBP",
                transaction_time=datetime(2024, 11, 10, 10, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Merchant",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="item-transfer",
                account_uid="acc-1",
                category_uid="space-1",
                space_uid="space-1",
                direction="OUT",
                amount_minor_units=-2000,
                currency="GBP",
                transaction_time=datetime(2024, 11, 11, 9, 0, tzinfo=timezone.utc),
                source="SAVINGS_GOAL",
                counterparty="",
                spending_category=None,
                classified_category=None,
                classification_reason=None,
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="item-main",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-3000,
                currency="GBP",
                transaction_time=datetime(2024, 11, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Grocer",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
        ]
    )


def test_spending_page_renders(settings):
    settings.STARLING_SUMMARY_DAYS = 30
    _seed_transactions()
    client = Client()
    response = client.get(reverse("spaces:spending"))
    assert response.status_code == 200
    markup = response.content.decode()
    assert "Spending Overview" in markup
    assert "href=\"/\"" in markup
    assert "days=365" in markup
    assert response.context["initial_category"] == ""


def test_spending_page_respects_custom_days(settings):
    _seed_transactions()
    client = Client()
    response = client.get(reverse("spaces:spending"), {"days": 180})
    assert response.status_code == 200
    assert "days=180" in response.content.decode()


def test_spending_page_prefills_from_category_path(settings):
    _seed_transactions()
    client = Client()
    response = client.get(reverse("spaces:spending-category", args=["Dining"]))
    assert response.status_code == 200
    assert response.context["initial_category"] == "Dining"


def test_spending_data_groups_by_spending_category(settings):
    _seed_transactions()
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
    assert payload["dates"] == ["2024-11-01"]

    assert payload["dates"] == ["2024-11-01"]

    assert payload["months"] == 1

    assert len(payload["series"]) == 1
    shopping_series = payload["series"][0]
    assert shopping_series["category"] == "Shopping"
    assert shopping_series["minorValues"] == [8000]
    assert shopping_series["values"] == [80.0]
    assert shopping_series["averageMinorUnits"] == 8000
    assert shopping_series["average"] == 80.0

    # ensure internal transfer excluded entirely
    totals = {item["category"]: item["totalMinorUnits"] for item in payload["series"]}
    assert totals["Shopping"] == 8000


def test_spending_data_rejects_invalid_days():
    client = Client()
    response = client.get(reverse("spaces:spending-data"), {"days": 0})
    assert response.status_code == 400


def test_spending_data_defaults_to_settings_window(settings):
    _seed_transactions()
    settings.STARLING_SUMMARY_DAYS = 120
    client = Client()
    response = client.get(reverse("spaces:spending-data"))
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["series"]
    assert payload["days"] == 365


def test_spending_data_rejects_invalid_reference():
    client = Client()
    response = client.get(reverse("spaces:spending-data"), {"reference": "not-a-date"})
    assert response.status_code == 400


def test_spending_data_handles_naive_reference(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-data"),
        {
            "reference": "2024-11-15T00:00:00",
            "days": 10,
        },
    )
    assert response.status_code == 200


def test_spending_data_rejects_non_numeric_days():
    client = Client()
    response = client.get(reverse("spaces:spending-data"), {"days": "many"})
    assert response.status_code == 400


def test_spending_transactions_returns_matching_rows(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "category": "Shopping",
            "days": 30,
            "reference": "2024-11-15T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["category"] == "Shopping"
    ids = [item["feedItemUid"] for item in payload["transactions"]]
    assert ids == ["item-main", "item-space"]
    assert payload["transactions"][0]["amountMinorUnits"] == 3000
    assert payload["transactions"][1]["amountMinorUnits"] == 5000
    assert payload["transactions"][0]["category"] == "Shopping"


def test_spending_transactions_requires_category():
    client = Client()
    response = client.get(reverse("spaces:spending-transactions"))
    assert response.status_code == 400


def test_spending_transactions_rejects_invalid_days():
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {"category": "Shopping", "days": 0},
    )
    assert response.status_code == 400


def test_calculate_spend_by_category_validates_args():
    with pytest.raises(ValueError):
        calculate_spend_by_category(days=0)


def test_calculate_spend_normalises_reference():
    _seed_transactions()
    summary = calculate_spend_by_category(
        days=10,
        reference_time=datetime(2024, 11, 15, 12, 0, 0),
    )
    assert summary["reference"].endswith("+00:00")
