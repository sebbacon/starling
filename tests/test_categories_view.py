import json
from datetime import datetime, timezone, timedelta

import pytest
from django.test import Client
from django.urls import reverse

from starling_web.spaces.models import FeedItem


pytestmark = pytest.mark.django_db


def _make_feed_item(
    *,
    feed_item_uid,
    amount_minor_units,
    category,
    when,
    source="CARD",
):
    FeedItem.objects.create(
        feed_item_uid=feed_item_uid,
        account_uid="acc-1",
        category_uid="cat-1",
        space_uid="",
        direction="OUT",
        amount_minor_units=-amount_minor_units,
        currency="GBP",
        transaction_time=when,
        source=source,
        counterparty="Vendor",
        spending_category=category,
        classified_category=category,
        classification_reason="test",
        raw_json={},
    )


def _seed_category_data():
    reference = datetime(2024, 11, 30, 12, 0, tzinfo=timezone.utc)
    # Past month items (within last 30 days)
    _make_feed_item(
        feed_item_uid="food-nov",
        amount_minor_units=5000,
        category="Food",
        when=reference - timedelta(days=10),
    )
    _make_feed_item(
        feed_item_uid="travel-nov",
        amount_minor_units=2000,
        category="Travel",
        when=reference - timedelta(days=5),
    )
    # Older than range
    _make_feed_item(
        feed_item_uid="food-sept",
        amount_minor_units=7000,
        category="Food",
        when=reference - timedelta(days=75),
    )
    # Internal transfer that should be excluded
    _make_feed_item(
        feed_item_uid="transfer",
        amount_minor_units=9000,
        category="Transfers",
        when=reference - timedelta(days=3),
        source="SAVINGS_GOAL",
    )
    # Additional history to exercise monthly average
    _make_feed_item(
        feed_item_uid="food-april",
        amount_minor_units=6000,
        category="Food",
        when=datetime(2024, 4, 15, 9, 0, tzinfo=timezone.utc),
    )
    _make_feed_item(
        feed_item_uid="travel-january",
        amount_minor_units=3000,
        category="Travel",
        when=datetime(2024, 1, 5, 9, 0, tzinfo=timezone.utc),
    )
    _make_feed_item(
        feed_item_uid="uncat-march",
        amount_minor_units=1200,
        category=None,
        when=datetime(2024, 3, 20, 9, 0, tzinfo=timezone.utc),
    )
    return reference


def test_categories_page_renders():
    client = Client()
    response = client.get(reverse("spaces:categories"))
    assert response.status_code == 200
    markup = response.content.decode()
    assert "Category Insights" in markup
    assert "Past 30 days" in markup
    assert "Monthly average" in markup


def test_categories_data_defaults_to_past_month():
    reference = _seed_category_data()
    client = Client()
    response = client.get(
        reverse("spaces:categories-data"),
        {"reference": reference.isoformat()},
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["period"] == "past_month"
    assert payload["metric"] == "total"
    categories = {row["category"]: row for row in payload["categories"]}
    assert set(categories) == {"Food", "Travel"}
    assert categories["Food"]["valueMinorUnits"] == 5000
    assert categories["Travel"]["valueMinorUnits"] == 2000
    assert payload["totalMinorUnits"] == 7000
    assert categories["Food"]["percentage"] == pytest.approx(71.428, rel=1e-3)


def test_categories_data_all_time_includes_full_history():
    reference = _seed_category_data()
    client = Client()
    response = client.get(
        reverse("spaces:categories-data"),
        {"period": "all_time", "reference": reference.isoformat()},
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["period"] == "all_time"
    assert payload["metric"] == "total"
    categories = {row["category"]: row for row in payload["categories"]}
    assert set(categories) == {"Food", "Travel", "Uncategorised"}
    assert categories["Food"]["totalMinorUnits"] == 18000
    assert categories["Travel"]["totalMinorUnits"] == 5000
    assert categories["Uncategorised"]["totalMinorUnits"] == 1200
    assert payload["totalMinorUnits"] == 24200


def test_categories_data_monthly_average_uses_spread_over_months():
    reference = _seed_category_data()
    client = Client()
    response = client.get(
        reverse("spaces:categories-data"),
        {"period": "monthly_average", "reference": reference.isoformat()},
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["period"] == "monthly_average"
    assert payload["metric"] == "average"
    assert payload["months"] == 11
    categories = {row["category"]: row for row in payload["categories"]}
    assert categories["Food"]["totalMinorUnits"] == 18000
    assert categories["Food"]["valueMinorUnits"] == pytest.approx(1636, rel=0.01)
    assert categories["Travel"]["valueMinorUnits"] == pytest.approx(454, rel=0.01)
    assert categories["Uncategorised"]["valueMinorUnits"] == pytest.approx(109, rel=0.01)
    assert payload["totalMinorUnits"] == pytest.approx(
        categories["Food"]["valueMinorUnits"]
        + categories["Travel"]["valueMinorUnits"]
        + categories["Uncategorised"]["valueMinorUnits"],
        rel=1e-6,
    )


def test_categories_data_rejects_invalid_period():
    client = Client()
    response = client.get(
        reverse("spaces:categories-data"),
        {"period": "bogus"},
    )
    assert response.status_code == 400
