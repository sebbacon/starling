import json
from datetime import datetime, timedelta, timezone

import pytest
from django.test import Client
from django.urls import reverse

from starling_web.spaces.models import FeedItem


pytestmark = pytest.mark.django_db


def _seed_cashflow_transactions():
    FeedItem.objects.bulk_create(
        [
            FeedItem(
                feed_item_uid="spend-nov",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-5000,
                currency="GBP",
                transaction_time=datetime(2024, 11, 10, 10, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Grocer",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="income-nov",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="IN",
                amount_minor_units=7000,
                currency="GBP",
                transaction_time=datetime(2024, 11, 15, 9, 0, tzinfo=timezone.utc),
                source="BANK_TRANSFER",
                counterparty="Employer",
                spending_category=None,
                classified_category="Salary/Expenses",
                classification_reason="manual",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="income-bonus-nov",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="IN",
                amount_minor_units=2000,
                currency="GBP",
                transaction_time=datetime(2024, 11, 16, 9, 0, tzinfo=timezone.utc),
                source="BANK_TRANSFER",
                counterparty="Refund",
                spending_category=None,
                classified_category="Refund",
                classification_reason="manual",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="spend-dec",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-3000,
                currency="GBP",
                transaction_time=datetime(2024, 12, 5, 10, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Cafe",
                spending_category="DINING",
                classified_category="Dining",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="excluded-transfer",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-2000,
                currency="GBP",
                transaction_time=datetime(2024, 11, 20, 10, 0, tzinfo=timezone.utc),
                source="SAVINGS_GOAL",
                counterparty="",
                spending_category=None,
                classified_category=None,
                classification_reason=None,
                raw_json={},
            ),
        ]
    )


def test_cashflow_page_renders():
    client = Client()
    response = client.get(reverse("spaces:cashflow"))
    assert response.status_code == 200
    markup = response.content.decode()
    assert "Cashflow overview" in markup
    assert "cashflow-net-delta" in markup
    assert "cashflow-average-net-delta" in markup
    assert "cashflow-monthly-alerts" in markup
    assert "cashflow-year-comparison" in markup
    assert "cashflow-year-comparison-table" in markup
    assert '<details class="cashflow-year-comparison" id="cashflow-year-comparison">' in markup
    assert "income-scope-toggle" in markup
    assert "cashflow-range-toggle" in markup
    assert "params.set('flow', currentFlow);" in markup
    assert "params.set('sort', sortState.column);" in markup
    assert response.context["spending_page_url"] == reverse("spaces:spending")
    assert response.context["income_page_url"] == reverse("spaces:income")
    assert response.context["cashflow_range"] == "year"


def test_cashflow_page_prefills_all_time_range():
    client = Client()
    response = client.get(reverse("spaces:cashflow"), {"range": "all"})
    assert response.status_code == 200
    assert response.context["cashflow_range"] == "all"


def test_cashflow_data_returns_monthly_spending_and_income():
    _seed_cashflow_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:cashflow-data"),
        {
            "start": "2024-11-01T00:00:00Z",
            "end": "2025-01-01T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["bucket"] == "month"
    assert payload["dates"] == ["2024-11-01", "2024-12-01"]
    assert payload["spendingMinorValues"] == [5000, 3000]
    assert payload["incomeMinorValues"] == [7000, 0]
    assert payload["incomeScope"] == "salary"
    assert payload["comparison"]["currentPeriod"]["label"] == "Jan 2024 to Dec 2024"
    assert payload["comparison"]["previousPeriod"]["label"] == "Jan 2023 to Dec 2023"


def test_cashflow_data_can_include_all_income():
    _seed_cashflow_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:cashflow-data"),
        {
            "start": "2024-11-01T00:00:00Z",
            "end": "2025-01-01T00:00:00Z",
            "income_scope": "all",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["incomeMinorValues"] == [9000, 0]
    assert payload["incomeScope"] == "all"


def test_cashflow_data_can_include_payments_only():
    _seed_cashflow_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:cashflow-data"),
        {
            "start": "2024-11-01T00:00:00Z",
            "end": "2025-01-01T00:00:00Z",
            "income_scope": "payments",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["incomeMinorValues"] == [2000, 0]
    assert payload["incomeScope"] == "payments"


def test_cashflow_data_can_use_all_time_range():
    _seed_cashflow_transactions()
    FeedItem.objects.create(
        feed_item_uid="spend-old",
        account_uid="acc-1",
        category_uid="cat-1",
        space_uid="",
        direction="OUT",
        amount_minor_units=-1200,
        currency="GBP",
        transaction_time=datetime(2023, 2, 1, 10, 0, tzinfo=timezone.utc),
        source="CARD",
        counterparty="Old Merchant",
        spending_category="SHOPPING",
        classified_category="Shopping",
        classification_reason="starling_fallback",
        raw_json={},
    )
    client = Client()
    response = client.get(
        reverse("spaces:cashflow-data"),
        {
            "range": "all",
            "reference": "2024-12-10T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["dates"][0] == "2023-02-01"
    assert payload["spendingMinorValues"][0] == 1200


def test_cashflow_data_includes_trailing_period_comparison():
    FeedItem.objects.bulk_create(
        [
            FeedItem(
                feed_item_uid="cashflow-2024-spend-jan",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-4000,
                currency="GBP",
                transaction_time=datetime(2024, 1, 11, 10, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Grocer",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="cashflow-2024-income-jan",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="IN",
                amount_minor_units=10000,
                currency="GBP",
                transaction_time=datetime(2024, 1, 12, 10, 0, tzinfo=timezone.utc),
                source="BANK_TRANSFER",
                counterparty="Employer",
                spending_category=None,
                classified_category="Salary/Expenses",
                classification_reason="manual",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="cashflow-2024-spend-feb",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-2000,
                currency="GBP",
                transaction_time=datetime(2024, 2, 11, 10, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Cafe",
                spending_category="DINING",
                classified_category="Dining",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="cashflow-2024-income-feb",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="IN",
                amount_minor_units=9000,
                currency="GBP",
                transaction_time=datetime(2024, 2, 12, 10, 0, tzinfo=timezone.utc),
                source="BANK_TRANSFER",
                counterparty="Employer",
                spending_category=None,
                classified_category="Salary/Expenses",
                classification_reason="manual",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="cashflow-2023-spend-jan",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-3000,
                currency="GBP",
                transaction_time=datetime(2023, 1, 11, 10, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Grocer",
                spending_category="SHOPPING",
                classified_category="Shopping",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="cashflow-2023-income-jan",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="IN",
                amount_minor_units=8000,
                currency="GBP",
                transaction_time=datetime(2023, 1, 12, 10, 0, tzinfo=timezone.utc),
                source="BANK_TRANSFER",
                counterparty="Employer",
                spending_category=None,
                classified_category="Salary/Expenses",
                classification_reason="manual",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="cashflow-2023-spend-feb",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="OUT",
                amount_minor_units=-1000,
                currency="GBP",
                transaction_time=datetime(2023, 2, 11, 10, 0, tzinfo=timezone.utc),
                source="CARD",
                counterparty="Cafe",
                spending_category="DINING",
                classified_category="Dining",
                classification_reason="starling_fallback",
                raw_json={},
            ),
            FeedItem(
                feed_item_uid="cashflow-2023-income-feb",
                account_uid="acc-1",
                category_uid="cat-1",
                space_uid="",
                direction="IN",
                amount_minor_units=7000,
                currency="GBP",
                transaction_time=datetime(2023, 2, 12, 10, 0, tzinfo=timezone.utc),
                source="BANK_TRANSFER",
                counterparty="Employer",
                spending_category=None,
                classified_category="Salary/Expenses",
                classification_reason="manual",
                raw_json={},
            ),
        ]
    )

    client = Client()
    response = client.get(
        reverse("spaces:cashflow-data"),
        {
            "reference": "2024-02-29T23:59:59Z",
        },
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    comparison = payload["comparison"]
    assert comparison["currentPeriod"] == {
        "label": "Mar 2023 to Feb 2024",
        "start": "2023-03-01T00:00:00+00:00",
        "end": "2024-03-01T00:00:00+00:00",
        "months": 12,
        "spendingMinorUnits": 6000,
        "spending": 60.0,
        "incomeMinorUnits": 19000,
        "income": 190.0,
        "netMinorUnits": 13000,
        "net": 130.0,
        "averageMonthlySpendingMinorUnits": 500,
        "averageMonthlySpending": 5.0,
        "averageMonthlyIncomeMinorUnits": 1583,
        "averageMonthlyIncome": 15.83,
        "averageMonthlyNetMinorUnits": 1083,
        "averageMonthlyNet": 10.83,
    }
    assert comparison["previousPeriod"] == {
        "label": "Mar 2022 to Feb 2023",
        "start": "2022-03-01T00:00:00+00:00",
        "end": "2023-03-01T00:00:00+00:00",
        "months": 12,
        "spendingMinorUnits": 4000,
        "spending": 40.0,
        "incomeMinorUnits": 15000,
        "income": 150.0,
        "netMinorUnits": 11000,
        "net": 110.0,
        "averageMonthlySpendingMinorUnits": 333,
        "averageMonthlySpending": 3.33,
        "averageMonthlyIncomeMinorUnits": 1250,
        "averageMonthlyIncome": 12.5,
        "averageMonthlyNetMinorUnits": 917,
        "averageMonthlyNet": 9.17,
    }
    assert len(comparison["monthByMonth"]) == 12
    assert comparison["monthByMonth"][-2:] == [
        {
            "label": "Jan 2024",
            "previousLabel": "Jan 2023",
            "currentPeriodSpendingMinorUnits": 4000,
            "currentPeriodSpending": 40.0,
            "currentPeriodIncomeMinorUnits": 10000,
            "currentPeriodIncome": 100.0,
            "currentPeriodNetMinorUnits": 6000,
            "currentPeriodNet": 60.0,
            "previousPeriodSpendingMinorUnits": 3000,
            "previousPeriodSpending": 30.0,
            "previousPeriodIncomeMinorUnits": 8000,
            "previousPeriodIncome": 80.0,
            "previousPeriodNetMinorUnits": 5000,
            "previousPeriodNet": 50.0,
        },
        {
            "label": "Feb 2024",
            "previousLabel": "Feb 2023",
            "currentPeriodSpendingMinorUnits": 2000,
            "currentPeriodSpending": 20.0,
            "currentPeriodIncomeMinorUnits": 9000,
            "currentPeriodIncome": 90.0,
            "currentPeriodNetMinorUnits": 7000,
            "currentPeriodNet": 70.0,
            "previousPeriodSpendingMinorUnits": 1000,
            "previousPeriodSpending": 10.0,
            "previousPeriodIncomeMinorUnits": 7000,
            "previousPeriodIncome": 70.0,
            "previousPeriodNetMinorUnits": 6000,
            "previousPeriodNet": 60.0,
        },
    ]


def test_cashflow_data_rejects_invalid_range():
    client = Client()
    response = client.get(reverse("spaces:cashflow-data"), {"range": "forever"})
    assert response.status_code == 400


def test_cashflow_transactions_defaults_to_both_flows():
    _seed_cashflow_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:cashflow-transactions"),
        {
            "days": 60,
            "reference": "2024-12-10T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["flow"] == "both"
    assert payload["incomeScope"] == "salary"
    ids = [item["feedItemUid"] for item in payload["transactions"]]
    assert ids == ["spend-dec", "income-nov", "spend-nov"]


def test_cashflow_transactions_can_include_all_income():
    _seed_cashflow_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:cashflow-transactions"),
        {
            "flow": "both",
            "income_scope": "all",
            "days": 60,
            "reference": "2024-12-10T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    ids = [item["feedItemUid"] for item in payload["transactions"]]
    assert ids == ["spend-dec", "income-bonus-nov", "income-nov", "spend-nov"]


def test_cashflow_transactions_can_include_payments_only():
    _seed_cashflow_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:cashflow-transactions"),
        {
            "flow": "both",
            "income_scope": "payments",
            "days": 60,
            "reference": "2024-12-10T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    ids = [item["feedItemUid"] for item in payload["transactions"]]
    assert ids == ["spend-dec", "income-bonus-nov", "spend-nov"]


def test_cashflow_transactions_can_filter_by_flow():
    _seed_cashflow_transactions()
    client = Client()

    spend_response = client.get(
        reverse("spaces:cashflow-transactions"),
        {
            "flow": "spending",
            "days": 60,
            "reference": "2024-12-10T00:00:00Z",
        },
    )
    assert spend_response.status_code == 200
    spend_payload = json.loads(spend_response.content.decode())
    spend_ids = [item["feedItemUid"] for item in spend_payload["transactions"]]
    assert spend_ids == ["spend-dec", "spend-nov"]

    income_response = client.get(
        reverse("spaces:cashflow-transactions"),
        {
            "flow": "income",
            "days": 60,
            "reference": "2024-12-10T00:00:00Z",
        },
    )
    assert income_response.status_code == 200
    income_payload = json.loads(income_response.content.decode())
    income_ids = [item["feedItemUid"] for item in income_payload["transactions"]]
    assert income_ids == ["income-nov"]

    payment_income_response = client.get(
        reverse("spaces:cashflow-transactions"),
        {
            "flow": "income",
            "income_scope": "payments",
            "days": 60,
            "reference": "2024-12-10T00:00:00Z",
        },
    )
    assert payment_income_response.status_code == 200
    payment_income_payload = json.loads(payment_income_response.content.decode())
    payment_income_ids = [item["feedItemUid"] for item in payment_income_payload["transactions"]]
    assert payment_income_ids == ["income-bonus-nov"]


def test_cashflow_transactions_can_use_all_time_range():
    _seed_cashflow_transactions()
    FeedItem.objects.create(
        feed_item_uid="income-old",
        account_uid="acc-1",
        category_uid="cat-1",
        space_uid="",
        direction="IN",
        amount_minor_units=1100,
        currency="GBP",
        transaction_time=datetime(2023, 2, 1, 10, 0, tzinfo=timezone.utc),
        source="BANK_TRANSFER",
        counterparty="Employer",
        spending_category=None,
        classified_category="Salary/Expenses",
        classification_reason="manual",
        raw_json={},
    )
    client = Client()
    response = client.get(
        reverse("spaces:cashflow-transactions"),
        {
            "range": "all",
            "reference": "2024-12-10T00:00:00Z",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    ids = [item["feedItemUid"] for item in payload["transactions"]]
    assert ids == ["spend-dec", "income-nov", "spend-nov", "income-old"]


def test_cashflow_transactions_sort_before_paginating():
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
        reverse("spaces:cashflow-transactions"),
        {
            "flow": "spending",
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


def test_cashflow_transactions_rejects_invalid_flow():
    client = Client()
    response = client.get(reverse("spaces:cashflow-transactions"), {"flow": "sideways"})
    assert response.status_code == 400


def test_cashflow_transactions_rejects_invalid_income_scope():
    client = Client()
    response = client.get(reverse("spaces:cashflow-transactions"), {"income_scope": "other"})
    assert response.status_code == 400


def test_cashflow_transactions_rejects_invalid_range():
    client = Client()
    response = client.get(reverse("spaces:cashflow-transactions"), {"range": "forever"})
    assert response.status_code == 400
