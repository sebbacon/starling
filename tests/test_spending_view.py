import json
from datetime import datetime, timedelta, timezone

import pytest
from django.test import Client
from django.urls import reverse

from starling_spaces.analytics import calculate_spend_by_category
from starling_web.spaces.models import Category, CounterpartyNote, FeedItem, TransactionNote


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
                spender="Seb",
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
                spender="Kim",
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
    assert "category-sidebar" in markup
    assert "category-select-all" in markup
    assert "category-clear-all" in markup
    assert "summary-total-spending" in markup
    assert "summary-average-monthly-spending" in markup
    assert "summary-average-annual-spending" in markup
    assert "spending-year-comparison" in markup
    assert "spending-year-comparison-table" in markup
    assert 'id="comparison-period-select"' in markup
    assert '<details class="year-comparison" id="spending-year-comparison">' in markup
    assert "split-by-card-used" in markup
    assert "chart-split-legend" in markup
    assert "sidebar-spender-filter" in markup
    assert 'id="spender-filter"' not in markup
    assert "/spending/notes/counterparty/" in markup
    assert "/spending/notes/transaction/" in markup
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


def test_spending_page_prefills_spender_query(settings):
    _seed_transactions()
    client = Client()
    response = client.get(reverse("spaces:spending"), {"spender": "kim"})
    assert response.status_code == 200
    assert response.context["initial_spender"] == "kim"


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


def test_spending_data_can_filter_by_spender(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-data"),
        {
            "reference": "2024-11-15T00:00:00Z",
            "days": 10,
            "spender": "kim",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["spender"] == "kim"
    assert len(payload["series"]) == 1
    assert payload["series"][0]["minorValues"] == [3000]
    assert payload["spenderSeries"]["kim"][0]["minorValues"] == [3000]


def test_spending_data_includes_spender_split_series(settings):
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
    assert payload["spenderSeries"]["seb"][0]["category"] == "Shopping"
    assert payload["spenderSeries"]["seb"][0]["minorValues"] == [5000]
    assert payload["spenderSeries"]["kim"][0]["minorValues"] == [3000]


def test_spending_data_rejects_invalid_days():
    client = Client()
    response = client.get(reverse("spaces:spending-data"), {"days": 0})
    assert response.status_code == 400


def test_spending_data_rejects_invalid_spender():
    client = Client()
    response = client.get(reverse("spaces:spending-data"), {"spender": "alex"})
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
    assert payload["summary"]["totalMinorUnits"] == 8000
    assert payload["summary"]["total"] == 80.0
    assert payload["summary"]["averageMonthlyMinorUnits"] == 8111
    assert payload["summary"]["averageMonthly"] == 81.11
    assert payload["summary"]["averageAnnualMinorUnits"] == 97333
    assert payload["summary"]["averageAnnual"] == 973.33
    assert payload["summary"]["periodDays"] == 30
    assert payload["summary"]["currency"] == "GBP"
    assert payload["summary"]["periodComparison"]["currentPeriod"]["label"] == "Nov 2023 to Oct 2024"
    assert payload["summary"]["periodComparison"]["previousPeriod"]["label"] == "Nov 2022 to Oct 2023"


def test_spending_transactions_include_trailing_period_comparison(settings):
    FeedItem.objects.bulk_create(
        [
            FeedItem(
                feed_item_uid="shopping-2024-jan",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-2000,
                currency="GBP",
                transaction_time=datetime(2024, 1, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="shopping-2024-feb",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-3000,
                currency="GBP",
                transaction_time=datetime(2024, 2, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="shopping-2024-mar",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-4000,
                currency="GBP",
                transaction_time=datetime(2024, 3, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="shopping-2023-jan",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-1000,
                currency="GBP",
                transaction_time=datetime(2023, 1, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="shopping-2023-feb",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-2000,
                currency="GBP",
                transaction_time=datetime(2023, 2, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="shopping-2023-mar",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-3000,
                currency="GBP",
                transaction_time=datetime(2023, 3, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
        ]
    )

    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "category": "Shopping",
            "days": 30,
            "reference": "2024-03-31T23:59:59Z",
        },
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    comparison = payload["summary"]["periodComparison"]
    assert comparison["currentPeriod"] == {
        "label": "Apr 2023 to Mar 2024",
        "start": "2023-04-01T00:00:00+00:00",
        "end": "2024-04-01T00:00:00+00:00",
        "totalMinorUnits": 9000,
        "total": 90.0,
        "monthlyAverageMinorUnits": 750,
        "monthlyAverage": 7.5,
        "months": 12,
    }
    assert comparison["previousPeriod"] == {
        "label": "Apr 2022 to Mar 2023",
        "start": "2022-04-01T00:00:00+00:00",
        "end": "2023-04-01T00:00:00+00:00",
        "totalMinorUnits": 6000,
        "total": 60.0,
        "monthlyAverageMinorUnits": 500,
        "monthlyAverage": 5.0,
        "months": 12,
    }
    assert len(comparison["monthByMonth"]) == 12
    assert comparison["monthByMonth"][-3:] == [
        {
            "label": "Jan 2024",
            "previousLabel": "Jan 2023",
            "currentPeriodMinorUnits": 2000,
            "currentPeriodTotal": 20.0,
            "previousPeriodMinorUnits": 1000,
            "previousPeriodTotal": 10.0,
        },
        {
            "label": "Feb 2024",
            "previousLabel": "Feb 2023",
            "currentPeriodMinorUnits": 3000,
            "currentPeriodTotal": 30.0,
            "previousPeriodMinorUnits": 2000,
            "previousPeriodTotal": 20.0,
        },
        {
            "label": "Mar 2024",
            "previousLabel": "Mar 2023",
            "currentPeriodMinorUnits": 4000,
            "currentPeriodTotal": 40.0,
            "previousPeriodMinorUnits": 3000,
            "previousPeriodTotal": 30.0,
        },
    ]


def test_spending_transactions_accept_comparison_reference_override(settings):
    FeedItem.objects.bulk_create(
        [
            FeedItem(
                feed_item_uid="shopping-2024-jan-override",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-2000,
                currency="GBP",
                transaction_time=datetime(2024, 1, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="shopping-2024-feb-override",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-3000,
                currency="GBP",
                transaction_time=datetime(2024, 2, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="shopping-2023-jan-override",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-1000,
                currency="GBP",
                transaction_time=datetime(2023, 1, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="shopping-2023-feb-override",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-2000,
                currency="GBP",
                transaction_time=datetime(2023, 2, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
        ]
    )

    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "category": "Shopping",
            "days": 30,
            "reference": "2024-03-31T23:59:59Z",
            "comparison_reference": "2024-02-29T23:59:59Z",
        },
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    comparison = payload["summary"]["periodComparison"]
    assert comparison["currentPeriod"]["label"] == "Mar 2023 to Feb 2024"
    assert comparison["previousPeriod"]["label"] == "Mar 2022 to Feb 2023"


def test_spending_transactions_can_filter_by_spender(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "category": "Shopping",
            "spender": "seb",
            "days": 30,
            "reference": "2024-11-15T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["spender"] == "seb"
    ids = [item["feedItemUid"] for item in payload["transactions"]]
    assert ids == ["item-space"]
    assert payload["summary"]["totalMinorUnits"] == 5000


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


def test_spending_transactions_can_filter_multiple_categories(settings):
    FeedItem.objects.bulk_create(
        [
            FeedItem(
                feed_item_uid="item-shopping",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-3000,
                currency="GBP",
                transaction_time=datetime(2024, 11, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="item-dining",
                account_uid="acc-1",
                category_uid="cat-2",
                space_uid="",
                direction="OUT",
                amount_minor_units=-4500,
                currency="GBP",
                transaction_time=datetime(2024, 11, 10, 13, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Cafe",
                spending_category="EATING_OUT",
                classified_category="Dining",
                classification_reason="starling_fallback",
                raw_json={},
            ),
        ]
    )
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "days": 30,
            "reference": "2024-11-15T00:00:00Z",
            "categories": ["Dining"],
        },
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert [item["feedItemUid"] for item in payload["transactions"]] == ["item-dining"]
    assert payload["transactions"][0]["category"] == "Dining"


def test_spending_transactions_apply_categories_and_spender_to_summary(settings):
    FeedItem.objects.bulk_create(
        [
            FeedItem(
                feed_item_uid="item-seb-dining",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-3000,
                currency="GBP",
                transaction_time=datetime(2024, 11, 10, 12, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Cafe One",
                spending_category="EATING_OUT",
                classified_category="Dining",
                classification_reason="starling_fallback",
                spender="Seb",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="item-kim-dining",
                account_uid="acc-1",
                category_uid="cat-2",
                space_uid="",
                direction="OUT",
                amount_minor_units=-4500,
                currency="GBP",
                transaction_time=datetime(2024, 11, 10, 13, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Cafe Two",
                spending_category="EATING_OUT",
                classified_category="Dining",
                classification_reason="starling_fallback",
                spender="Kim",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="item-seb-shopping",
                account_uid="acc-1",
                category_uid="cat-3",
                space_uid="",
                direction="OUT",
                amount_minor_units=-7000,
                currency="GBP",
                transaction_time=datetime(2024, 11, 10, 14, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Shop",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                spender="Seb",
                raw_json={},
            ),
        ]
    )
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "days": 30,
            "reference": "2024-11-15T00:00:00Z",
            "categories": ["Dining"],
            "spender": "seb",
        },
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert [item["feedItemUid"] for item in payload["transactions"]] == ["item-seb-dining"]
    assert payload["summary"]["totalMinorUnits"] == 3000


def test_spending_transactions_return_empty_result_for_empty_category_selection(settings):
    _seed_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "days": 30,
            "reference": "2024-11-15T00:00:00Z",
            "categories": ["__none__"],
        },
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["transactions"] == []
    assert payload["totalCount"] == 0
    assert payload["summary"]["totalMinorUnits"] == 0


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


def test_spending_transactions_include_notes(settings):
    _seed_transactions()
    CounterpartyNote.objects.create(
        counterparty="Merchant",
        counterparty_key="merchant",
        note="Shared counterparty note",
    )
    TransactionNote.objects.create(
        feed_item_id="item-main",
        note="Transaction-specific note",
    )

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
    notes_by_id = {item["feedItemUid"]: item for item in payload["transactions"]}
    assert notes_by_id["item-space"]["counterpartyNote"] == "Shared counterparty note"
    assert notes_by_id["item-space"]["spender"] == "Seb"
    assert notes_by_id["item-space"]["transactionNote"] == ""
    assert notes_by_id["item-main"]["counterpartyNote"] == ""
    assert notes_by_id["item-main"]["spender"] == "Kim"
    assert notes_by_id["item-main"]["transactionNote"] == "Transaction-specific note"


def test_spending_counterparty_note_can_be_saved_and_cleared(settings):
    _seed_transactions()
    client = Client(enforce_csrf_checks=False)

    create_response = client.post(
        reverse("spaces:spending-counterparty-note"),
        data=json.dumps(
            {
                "counterparty": "Merchant",
                "note": "Remember this is the local bakery",
            }
        ),
        content_type="application/json",
    )

    assert create_response.status_code == 200
    create_payload = json.loads(create_response.content.decode())
    assert create_payload["note"] == "Remember this is the local bakery"
    assert CounterpartyNote.objects.get(counterparty_key="merchant").note == "Remember this is the local bakery"

    clear_response = client.post(
        reverse("spaces:spending-counterparty-note"),
        data=json.dumps(
            {
                "counterparty": "Merchant",
                "note": "",
            }
        ),
        content_type="application/json",
    )

    assert clear_response.status_code == 200
    clear_payload = json.loads(clear_response.content.decode())
    assert clear_payload["note"] == ""
    assert CounterpartyNote.objects.filter(counterparty_key="merchant").count() == 0


def test_spending_transaction_note_can_be_saved_and_cleared(settings):
    _seed_transactions()
    client = Client(enforce_csrf_checks=False)

    create_response = client.post(
        reverse("spaces:spending-transaction-note"),
        data=json.dumps(
            {
                "feedItemUid": "item-main",
                "note": "Refund expected next week",
            }
        ),
        content_type="application/json",
    )

    assert create_response.status_code == 200
    create_payload = json.loads(create_response.content.decode())
    assert create_payload["note"] == "Refund expected next week"
    assert TransactionNote.objects.get(feed_item_id="item-main").note == "Refund expected next week"

    clear_response = client.post(
        reverse("spaces:spending-transaction-note"),
        data=json.dumps(
            {
                "feedItemUid": "item-main",
                "note": "",
            }
        ),
        content_type="application/json",
    )

    assert clear_response.status_code == 200
    clear_payload = json.loads(clear_response.content.decode())
    assert clear_payload["note"] == ""
    assert TransactionNote.objects.filter(feed_item_id="item-main").count() == 0


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


def test_spending_transactions_sort_before_paginating(settings):
    base_time = datetime(2024, 11, 15, 12, 0, tzinfo=timezone.utc)
    FeedItem.objects.bulk_create(
        [
            FeedItem(
                feed_item_uid=f"item-{index:03d}",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-(205 - index),
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
    response = client.get(
        reverse("spaces:spending-transactions"),
        {
            "days": 30,
            "reference": "2024-11-16T00:00:00Z",
            "sort": "amountMinorUnits",
            "direction": "asc",
            "page": 2,
        },
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["count"] == 5
    assert payload["page"] == 2
    assert payload["totalPages"] == 2

    ids = [item["feedItemUid"] for item in payload["transactions"]]
    assert ids == ["item-004", "item-003", "item-002", "item-001", "item-000"]
    amounts = [item["amountMinorUnits"] for item in payload["transactions"]]
    assert amounts == [201, 202, 203, 204, 205]


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
