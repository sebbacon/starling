import json
from datetime import datetime, timezone

import pytest
from django.test import Client
from django.urls import reverse

from starling_web.spaces.models import FeedItem


pytestmark = pytest.mark.django_db


def _make_spend(*, uid, when, amount_minor_units, counterparty, category, source="CARD"):
    return FeedItem(
        feed_item_uid=uid,
        account_uid="acc-1",
        category_uid="cat-1",
        space_uid="",
        direction="OUT",
        amount_minor_units=amount_minor_units,
        currency="GBP",
        transaction_time=when,
        source=source,
        counterparty=counterparty,
        spending_category="SHOPPING",
        classified_category=category,
        classification_reason="starling_fallback",
        raw_json={},
    )


def _seed_savings_transactions():
    records = []

    for month in [6, 7, 8, 9, 10, 11, 12]:
        records.append(
            _make_spend(
                uid=f"netflix-{month}",
                when=datetime(2024, month, 5, 9, 0, tzinfo=timezone.utc),
                amount_minor_units=-1299,
                counterparty="Netflix",
                category="Entertainment",
            )
        )

    trend_months = [7, 8, 9, 10, 11, 12]
    trend_amounts = [3000, 3200, 3400, 6200, 7000, 7800]
    for month, amount in zip(trend_months, trend_amounts):
        records.append(
            _make_spend(
                uid=f"grocer-{month}",
                when=datetime(2024, month, 9, 10, 0, tzinfo=timezone.utc),
                amount_minor_units=-amount,
                counterparty="GrocerMart",
                category="Groceries",
            )
        )

    cafe_amounts = [500, 550, 520, 530, 4500]
    cafe_days = [1, 8, 15, 22, 20]
    cafe_months = [8, 8, 8, 8, 12]
    for index, amount in enumerate(cafe_amounts):
        records.append(
            _make_spend(
                uid=f"cafe-{index}",
                when=datetime(2024, cafe_months[index], cafe_days[index], 12, 0, tzinfo=timezone.utc),
                amount_minor_units=-amount,
                counterparty="Cafe Spot",
                category="Eating Out",
            )
        )

    records.append(
        _make_spend(
            uid="excluded-transfer",
            when=datetime(2024, 12, 30, 8, 0, tzinfo=timezone.utc),
            amount_minor_units=-9999,
            counterparty="Savings Sweep",
            category="Transfers",
            source="SAVINGS_GOAL",
        )
    )

    FeedItem.objects.bulk_create(records)


def test_savings_page_renders():
    client = Client()
    response = client.get(reverse("spaces:savings"))
    assert response.status_code == 200
    markup = response.content.decode()
    assert "savings signals" in markup.lower()
    assert "signal-group-toggle" in markup
    assert response.context["data_endpoint"] == reverse("spaces:savings-data")


def test_savings_data_returns_subscription_trend_and_anomaly_signals():
    _seed_savings_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:savings-data"),
        {
            "reference": "2025-01-15T00:00:00Z",
            "days": 365,
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["confidenceMode"] == "balanced"
    assert payload["group"] == "all"
    assert payload["summary"]["signalsCount"] > 0
    assert payload["summary"]["potentialMonthlySavingsMinor"] > 0
    assert payload["summary"]["potentialAnnualSavingsMinor"] > 0

    signals = payload["signals"]
    types = {signal["type"] for signal in signals}
    assert {"subscription", "trend", "anomaly"} <= types

    subscription = next(
        signal for signal in signals
        if signal["type"] == "subscription" and signal["counterparty"] == "Netflix"
    )
    assert subscription["impactMonthlyMinor"] > 0
    assert subscription["drilldownUrl"].startswith("/spending/")
    assert "start=" in subscription["drilldownUrl"]
    assert "end=" in subscription["drilldownUrl"]

    assert any(
        signal["type"] == "trend"
        and (signal["category"] == "Groceries" or signal["counterparty"] == "GrocerMart")
        for signal in signals
    )
    assert any(
        signal["type"] == "anomaly"
        and signal["counterparty"] == "Cafe Spot"
        for signal in signals
    )
    assert all(signal.get("counterparty") != "Savings Sweep" for signal in signals)


def test_savings_data_can_filter_by_group():
    _seed_savings_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:savings-data"),
        {
            "reference": "2025-01-15T00:00:00Z",
            "days": 365,
            "group": "subscriptions",
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["group"] == "subscriptions"
    assert payload["signals"]
    assert all(signal["type"] == "subscription" for signal in payload["signals"])


def test_savings_data_rejects_invalid_confidence():
    client = Client()
    response = client.get(reverse("spaces:savings-data"), {"confidence": "nope"})
    assert response.status_code == 400


def test_savings_data_rejects_invalid_group():
    client = Client()
    response = client.get(reverse("spaces:savings-data"), {"group": "everything"})
    assert response.status_code == 400
