import json
from datetime import datetime, timedelta, timezone

import pytest
from django.test import Client
from django.urls import reverse

from starling_web.spaces.models import FeedItem


pytestmark = pytest.mark.django_db


def _make_item(
    *,
    uid,
    when,
    amount_minor_units,
    classified_category=None,
    source="CARD",
):
    return FeedItem(
        feed_item_uid=uid,
        account_uid="acc-1",
        category_uid="cat-1",
        space_uid="",
        direction="OUT" if amount_minor_units < 0 else "IN",
        amount_minor_units=amount_minor_units,
        currency="GBP",
        transaction_time=when,
        source=source,
        counterparty="Merchant",
        spending_category="SHOPPING",
        classified_category=classified_category,
        classification_reason="starling_fallback" if classified_category else None,
        raw_json={},
    )


def test_things_to_do_page_renders():
    client = Client()
    response = client.get(reverse("spaces:things-to-do"))
    assert response.status_code == 200
    assert "things to do" in response.content.decode().lower()


def test_things_to_do_transactions_returns_uncategorised_only():
    FeedItem.objects.bulk_create(
        [
            _make_item(
                uid="uncat-out",
                when=datetime(2024, 11, 10, 10, 0, tzinfo=timezone.utc),
                amount_minor_units=-5000,
                classified_category=None,
            ),
            _make_item(
                uid="uncat-in",
                when=datetime(2024, 11, 11, 10, 0, tzinfo=timezone.utc),
                amount_minor_units=2500,
                classified_category=None,
            ),
            _make_item(
                uid="categorised",
                when=datetime(2024, 11, 12, 10, 0, tzinfo=timezone.utc),
                amount_minor_units=-3000,
                classified_category="Shopping",
            ),
            _make_item(
                uid="uncat-literal",
                when=datetime(2024, 11, 11, 11, 0, tzinfo=timezone.utc),
                amount_minor_units=-1200,
                classified_category="Uncategorised",
            ),
            _make_item(
                uid="uncat-empty",
                when=datetime(2024, 11, 11, 9, 30, tzinfo=timezone.utc),
                amount_minor_units=-900,
                classified_category="",
            ),
            _make_item(
                uid="transfer",
                when=datetime(2024, 11, 13, 10, 0, tzinfo=timezone.utc),
                amount_minor_units=-4000,
                classified_category=None,
                source="SAVINGS_GOAL",
            ),
        ]
    )

    client = Client()
    response = client.get(reverse("spaces:things-to-do-transactions"))
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["count"] == 4
    assert payload["totalCount"] == 4
    assert payload["page"] == 1
    assert payload["pageSize"] == 200
    assert payload["totalPages"] == 1
    ids = [item["feedItemUid"] for item in payload["transactions"]]
    assert ids == ["uncat-literal", "uncat-in", "uncat-empty", "uncat-out"]


def test_things_to_do_transactions_paginates_at_200():
    base_time = datetime(2024, 11, 15, 12, 0, tzinfo=timezone.utc)
    FeedItem.objects.bulk_create(
        [
            _make_item(
                uid=f"uncat-{index:03d}",
                when=base_time - timedelta(minutes=index),
                amount_minor_units=-(index + 1),
                classified_category=None,
            )
            for index in range(205)
        ]
    )

    client = Client()
    page_one = client.get(reverse("spaces:things-to-do-transactions"))
    assert page_one.status_code == 200
    payload_one = json.loads(page_one.content.decode())
    assert payload_one["count"] == 200
    assert payload_one["totalCount"] == 205
    assert payload_one["totalPages"] == 2
    assert payload_one["hasNextPage"] is True
    ids_one = [item["feedItemUid"] for item in payload_one["transactions"]]
    assert ids_one[0] == "uncat-000"
    assert ids_one[-1] == "uncat-199"

    page_two = client.get(reverse("spaces:things-to-do-transactions"), {"page": 2})
    assert page_two.status_code == 200
    payload_two = json.loads(page_two.content.decode())
    assert payload_two["count"] == 5
    assert payload_two["totalCount"] == 205
    assert payload_two["page"] == 2
    assert payload_two["hasPreviousPage"] is True
    assert payload_two["hasNextPage"] is False
    ids_two = [item["feedItemUid"] for item in payload_two["transactions"]]
    assert ids_two == ["uncat-200", "uncat-201", "uncat-202", "uncat-203", "uncat-204"]
