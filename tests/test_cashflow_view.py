import json
from datetime import datetime, timezone

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
    assert "cashflow-monthly-alerts" in markup
    assert "income-scope-toggle" in markup
    assert response.context["spending_page_url"] == reverse("spaces:spending")
    assert response.context["income_page_url"] == reverse("spaces:income")


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


def test_cashflow_transactions_rejects_invalid_flow():
    client = Client()
    response = client.get(reverse("spaces:cashflow-transactions"), {"flow": "sideways"})
    assert response.status_code == 400


def test_cashflow_transactions_rejects_invalid_income_scope():
    client = Client()
    response = client.get(reverse("spaces:cashflow-transactions"), {"income_scope": "other"})
    assert response.status_code == 400
