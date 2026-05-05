import json
from datetime import datetime, timezone

import pytest
from django.test import Client
from django.urls import reverse

from starling_web.spaces.models import (
    Category,
    FeedItem,
    HolidayMerchantOverride,
    HolidaySuggestionDecision,
)


pytestmark = pytest.mark.django_db


def _make_spend(*, uid, when, amount_minor_units, counterparty, category, raw_json=None):
    return FeedItem(
        feed_item_uid=uid,
        account_uid="acc-1",
        category_uid="cat-1",
        space_uid="",
        direction="OUT",
        amount_minor_units=amount_minor_units,
        currency="GBP",
        transaction_time=when,
        source="MASTER_CARD",
        counterparty=counterparty,
        spending_category=category.upper().replace(" ", "_"),
        classified_category=category,
        classification_reason="starling_fallback",
        raw_json=raw_json or {},
    )


def _seed_holiday_transactions():
    Category.objects.get_or_create(
        account_uid="acc-cat",
        category_type="spending",
        category_uid="cat-holidays",
        defaults={"name": "Holidays"},
    )
    FeedItem.objects.bulk_create(
        [
            _make_spend(
                uid="holiday-1",
                when=datetime(2026, 2, 21, 11, 10, tzinfo=timezone.utc),
                amount_minor_units=-805,
                counterparty="TfL",
                category="Transport",
                raw_json={"country": "GB", "reference": "TFL TRAVEL CH"},
            ),
            _make_spend(
                uid="holiday-2",
                when=datetime(2026, 2, 21, 13, 2, tzinfo=timezone.utc),
                amount_minor_units=-9129,
                counterparty="Saikou Japanese Restau",
                category="Holidays",
                raw_json={"country": "GB"},
            ),
            _make_spend(
                uid="holiday-3",
                when=datetime(2026, 2, 21, 21, 34, tzinfo=timezone.utc),
                amount_minor_units=-2334,
                counterparty="Uber",
                category="Transport",
                raw_json={"country": "GB"},
            ),
        ]
    )


def test_holiday_suggestions_page_renders():
    client = Client()
    response = client.get(reverse("spaces:holidays"))
    assert response.status_code == 200
    markup = response.content.decode()
    assert "Holiday suggestions" in markup
    assert "Accept this cluster" in markup
    assert "Select all in cluster" in markup
    assert "Lookback" in markup
    assert "All available" in markup
    assert response.context["data_endpoint"] == reverse("spaces:holidays-data")


def test_holiday_suggestions_page_includes_earliest_transaction_time():
    _seed_holiday_transactions()
    client = Client()
    response = client.get(reverse("spaces:holidays"))
    assert response.status_code == 200
    assert response.context["earliest_transaction_time"].startswith("2026-02-21T11:10:00")


def test_holiday_suggestions_data_returns_clusters():
    _seed_holiday_transactions()
    client = Client()
    response = client.get(
        reverse("spaces:holidays-data"),
        {
            "reference": "2026-02-28T00:00:00Z",
            "days": 60,
        },
    )
    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["summary"]["clustersCount"] == 1
    assert payload["clusters"][0]["transactionCount"] == 3
    assert payload["clusters"][0]["transactions"][0]["feedItemUid"]


def test_holiday_suggestions_data_rejects_invalid_scope():
    client = Client()
    response = client.get(reverse("spaces:holidays-data"), {"scope": "moon"})
    assert response.status_code == 400


def test_holiday_feedback_persists_review_decisions():
    _seed_holiday_transactions()
    client = Client()

    response = client.post(
        reverse("spaces:holidays-feedback"),
        data=json.dumps(
            {
                "feedItemUids": ["holiday-1", "holiday-2"],
                "decision": "accepted",
            }
        ),
        content_type="application/json",
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["updated"] == 2
    assert {
        (item.feed_item_uid, item.decision)
        for item in HolidaySuggestionDecision.objects.order_by("feed_item_uid")
    } == {
        ("holiday-1", "accepted"),
        ("holiday-2", "accepted"),
    }


def test_holiday_merchant_override_persists_and_changes_payload():
    _seed_holiday_transactions()
    client = Client()

    response = client.post(
        reverse("spaces:holidays-merchant-overrides"),
        data=json.dumps(
            {
                "merchants": [
                    {
                        "merchantKey": "tfl",
                        "label": "TfL",
                    }
                ],
                "overrideType": "ignore",
            }
        ),
        content_type="application/json",
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["updated"] == 1
    assert list(
        HolidayMerchantOverride.objects.values_list("merchant_key", "label", "override_type")
    ) == [("tfl", "TfL", "ignore")]

    data_response = client.get(
        reverse("spaces:holidays-data"),
        {
            "reference": "2026-02-28T00:00:00Z",
            "days": 60,
        },
    )
    assert data_response.status_code == 200
    data_payload = json.loads(data_response.content.decode())
    assert data_payload["summary"]["clustersCount"] == 1
    assert {item["feedItemUid"] for item in data_payload["clusters"][0]["transactions"]} == {
        "holiday-2",
        "holiday-3",
    }


def test_holiday_suggestions_data_includes_review_decision():
    _seed_holiday_transactions()
    HolidaySuggestionDecision.objects.create(feed_item_uid="holiday-2", decision="rejected")
    client = Client()

    response = client.get(
        reverse("spaces:holidays-data"),
        {
            "reference": "2026-02-28T00:00:00Z",
            "days": 60,
        },
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    decisions = {
        item["feedItemUid"]: item["reviewDecision"]
        for item in payload["clusters"][0]["transactions"]
    }
    assert decisions["holiday-2"] == "rejected"


def test_holiday_suggestions_hides_accepted_items_by_default():
    _seed_holiday_transactions()
    HolidaySuggestionDecision.objects.create(feed_item_uid="holiday-1", decision="accepted")
    HolidaySuggestionDecision.objects.create(feed_item_uid="holiday-2", decision="accepted")
    HolidaySuggestionDecision.objects.create(feed_item_uid="holiday-3", decision="accepted")
    client = Client()

    response = client.get(
        reverse("spaces:holidays-data"),
        {
            "reference": "2026-02-28T00:00:00Z",
            "days": 60,
        },
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["summary"]["clustersCount"] == 0

    reviewed_response = client.get(
        reverse("spaces:holidays-data"),
        {
            "reference": "2026-02-28T00:00:00Z",
            "days": 60,
            "show_reviewed": "1",
        },
    )

    assert reviewed_response.status_code == 200
    reviewed_payload = json.loads(reviewed_response.content.decode())
    assert reviewed_payload["summary"]["clustersCount"] == 1
    assert reviewed_payload["clusters"][0]["suggestedTransactionCount"] == 0
    assert {
        item["feedItemUid"]: item["reviewDecision"]
        for item in reviewed_payload["clusters"][0]["transactions"]
    } == {
        "holiday-1": "accepted",
        "holiday-2": "accepted",
        "holiday-3": "accepted",
    }
