import json
from datetime import datetime, timedelta, timezone

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
    assert response.context["summary_days"] == 365
    assert "overview-container" in markup
    assert "top-category-slider" in markup
    assert "Top 10 categories" in markup
    assert response.context["initial_category"] == ""
    assert response.context["initial_counterparty"] == ""


def test_spending_page_respects_custom_days(settings):
    _seed_transactions()
    client = Client()
    response = client.get(reverse("spaces:spending"), {"days": 180})
    assert response.status_code == 200
    assert response.context["summary_days"] == 180


def test_spending_page_prefills_from_category_path(settings):
    _seed_transactions()
    client = Client()
    response = client.get(reverse("spaces:spending-category", args=["Dining"]))
    assert response.status_code == 200
    assert response.context["initial_category"] == "Dining"
    assert response.context["initial_counterparty"] == ""


def test_spending_page_prefills_counterparty_path(settings):
    _seed_transactions()
    client = Client()
    response = client.get(reverse("spaces:spending-counterparty", args=["Merchant"]))
    assert response.status_code == 200
    assert response.context["initial_counterparty"] == "Merchant"
    assert response.context["initial_category"] == ""


def test_spending_page_prefills_search_query(settings):
    _seed_transactions()
    client = Client()
    response = client.get(reverse("spaces:spending"), {"search": "Merchant"})
    assert response.status_code == 200
    assert response.context["initial_search"] == "Merchant"


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
    assert payload["bucket"] == "day"
    assert payload["dates"] == ["2024-11-10"]
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
    response = client.get(reverse("spaces:spending-data"), {"reference": "2024-11-15T00:00:00Z"})
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


def test_spending_data_accepts_date_range(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-data"),
        {
            "start": "2024-10-01T00:00:00Z",
            "end": "2024-12-01T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["start"].startswith("2024-10-01")
    assert payload["end"].startswith("2024-12-01")
    assert payload["days"] >= 60


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


def test_spending_transactions_can_filter_counterparty(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "counterparty": "Merchant",
            "days": 30,
            "reference": "2024-11-15T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["counterparty"] == "Merchant"
    assert payload["count"] == 1
    assert payload["transactions"][0]["counterparty"] == "Merchant"


def test_spending_transactions_can_search_counterparty(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "search": "Merchant",
            "days": 30,
            "reference": "2024-11-15T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["count"] == 1
    assert payload["transactions"][0]["counterparty"] == "Merchant"


def test_spending_transactions_can_search_amount(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "search": "30",
            "days": 30,
            "reference": "2024-11-15T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    ids = [txn["feedItemUid"] for txn in payload["transactions"]]
    assert ids == ["item-main"]


def test_spending_transactions_accepts_date_range(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "category": "Shopping",
            "start": "2024-11-01T00:00:00Z",
            "end": "2024-12-01T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["count"] == 2
    assert payload["start"].startswith("2024-11-01")
    assert payload["end"].startswith("2024-12-01")


def test_spending_transactions_recategorise_updates(settings):
    _seed_transactions()
    client = Client(enforce_csrf_checks=False)
    response = client.post(
        reverse("spaces:spending-recategorise"),
        data=json.dumps(
            {
                "feedItemUids": ["item-main"],
                "category": "Shopping",
            }
        ),
        content_type="application/json",
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["updated"] == 1

    item = FeedItem.objects.get(feed_item_uid="item-main")
    assert item.classified_category == "Shopping"
    assert item.classification_reason == "manual"


def test_spending_transactions_defaults_to_all_rows(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "days": 30,
            "reference": "2024-11-15T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    ids = [item["feedItemUid"] for item in payload["transactions"]]
    assert ids == ["item-main", "item-space"]
    assert payload["count"] == 2
    assert payload["totalCount"] == 2
    assert payload["page"] == 1
    assert payload["pageSize"] == 200
    assert payload["totalPages"] == 1
    assert payload["hasNextPage"] is False
    assert payload["hasPreviousPage"] is False


def test_spending_transactions_paginates_default_results(settings):
    base_time = datetime(2024, 11, 15, 12, 0, tzinfo=timezone.utc)
    FeedItem.objects.bulk_create(
        [
            FeedItem(
                feed_item_uid=f"item-{index:03d}",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-(index + 1),
                currency="GBP",
                transaction_time=base_time - timedelta(minutes=index),
                source="CARD",
                counterparty=f"Merchant {index}",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            )
            for index in range(205)
        ]
    )

    client = Client()
    page_one = client.get(
        reverse("spaces:spending-transactions"),
        {
            "days": 30,
            "reference": "2024-11-16T00:00:00Z",
        },
    )
    assert page_one.status_code == 200
    page_one_payload = json.loads(page_one.content.decode())
    assert page_one_payload["count"] == 200
    assert page_one_payload["totalCount"] == 205
    assert page_one_payload["page"] == 1
    assert page_one_payload["pageSize"] == 200
    assert page_one_payload["totalPages"] == 2
    assert page_one_payload["hasNextPage"] is True
    assert page_one_payload["hasPreviousPage"] is False
    page_one_ids = [item["feedItemUid"] for item in page_one_payload["transactions"]]
    assert page_one_ids[0] == "item-000"
    assert page_one_ids[-1] == "item-199"

    page_two = client.get(
        reverse("spaces:spending-transactions"),
        {
            "days": 30,
            "reference": "2024-11-16T00:00:00Z",
            "page": 2,
        },
    )
    assert page_two.status_code == 200
    page_two_payload = json.loads(page_two.content.decode())
    assert page_two_payload["count"] == 5
    assert page_two_payload["totalCount"] == 205
    assert page_two_payload["page"] == 2
    assert page_two_payload["totalPages"] == 2
    assert page_two_payload["hasNextPage"] is False
    assert page_two_payload["hasPreviousPage"] is True
    page_two_ids = [item["feedItemUid"] for item in page_two_payload["transactions"]]
    assert page_two_ids == ["item-200", "item-201", "item-202", "item-203", "item-204"]


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
