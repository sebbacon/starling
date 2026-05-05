from datetime import datetime, timezone

import pytest

from starling_spaces.holiday_signals import calculate_holiday_signals
from starling_web.spaces.models import FeedItem, HolidayMerchantOverride, HolidaySuggestionDecision


pytestmark = pytest.mark.django_db


def _make_spend(
    *,
    uid,
    when,
    amount_minor_units,
    counterparty,
    category,
    source="MASTER_CARD",
    raw_json=None,
):
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
        spending_category=category.upper().replace(" ", "_"),
        classified_category=category,
        classification_reason="starling_fallback",
        raw_json=raw_json or {},
    )


def _raw_payload(*, country="GB", reference=None, source_currency="GBP", minor_units=None):
    payload = {
        "country": country,
        "sourceAmount": {
            "currency": source_currency,
            "minorUnits": minor_units or 0,
        },
    }
    if reference:
        payload["reference"] = reference
    return payload


def test_holiday_signals_detects_foreign_trip_cluster():
    FeedItem.objects.bulk_create(
        [
            _make_spend(
                uid="mx-1",
                when=datetime(2025, 8, 1, 10, 0, tzinfo=timezone.utc),
                amount_minor_units=-3003,
                counterparty="Clip Mx*vegan Barrio",
                category="Eating Out",
                raw_json=_raw_payload(country="MX", source_currency="MXN", minor_units=74250),
            ),
            _make_spend(
                uid="mx-2",
                when=datetime(2025, 8, 1, 12, 0, tzinfo=timezone.utc),
                amount_minor_units=-5316,
                counterparty="Mega Cozumel",
                category="Groceries",
                raw_json=_raw_payload(country="MX", source_currency="MXN", minor_units=131500),
            ),
            _make_spend(
                uid="mx-3",
                when=datetime(2025, 8, 2, 18, 0, tzinfo=timezone.utc),
                amount_minor_units=-9414,
                counterparty="Cash Machine (Mexico)",
                category="Transport",
                raw_json=_raw_payload(country="MX", source_currency="MXN", minor_units=232900),
            ),
        ]
    )

    payload = calculate_holiday_signals(
        days=60,
        reference_time=datetime(2025, 8, 31, tzinfo=timezone.utc),
    )

    assert payload["summary"]["clustersCount"] == 1
    cluster = payload["clusters"][0]
    assert cluster["confidenceBand"] == "high"
    assert "foreign_spend" in cluster["reasonCodes"]
    assert {item["feedItemUid"] for item in cluster["transactions"]} == {"mx-1", "mx-2", "mx-3"}
    assert all(item["suggested"] for item in cluster["transactions"])


def test_holiday_signals_detects_domestic_anchor_led_cluster():
    FeedItem.objects.bulk_create(
        [
            _make_spend(
                uid="trip-1",
                when=datetime(2025, 4, 23, 18, 0, tzinfo=timezone.utc),
                amount_minor_units=-27598,
                counterparty="Airbnb",
                category="Holidays",
                raw_json=_raw_payload(country="GB", reference="AIRBNB * HMTEST123"),
            ),
            _make_spend(
                uid="trip-2",
                when=datetime(2025, 4, 25, 12, 0, tzinfo=timezone.utc),
                amount_minor_units=-13110,
                counterparty="Rp*food Now",
                category="Eating Out",
                raw_json=_raw_payload(country="GB"),
            ),
            _make_spend(
                uid="trip-3",
                when=datetime(2025, 4, 25, 16, 0, tzinfo=timezone.utc),
                amount_minor_units=-3881,
                counterparty="Murco Holmfirth Fillin Huddersfield Gbr",
                category="Transport",
                raw_json=_raw_payload(country="GB"),
            ),
            _make_spend(
                uid="trip-4",
                when=datetime(2025, 4, 26, 10, 0, tzinfo=timezone.utc),
                amount_minor_units=-2030,
                counterparty="Marks & Spencer",
                category="Groceries",
                raw_json=_raw_payload(country="GB"),
            ),
        ]
    )

    payload = calculate_holiday_signals(
        days=60,
        reference_time=datetime(2025, 5, 31, tzinfo=timezone.utc),
    )

    assert payload["summary"]["clustersCount"] == 1
    cluster = payload["clusters"][0]
    assert "accommodation_anchor" in cluster["reasonCodes"]
    assert cluster["suggestedTransactionCount"] == 4
    assert cluster["start"].startswith("2025-04-23")
    assert cluster["end"].startswith("2025-04-26")


def test_holiday_signals_suppresses_home_local_burst_without_anchor():
    FeedItem.objects.bulk_create(
        [
            _make_spend(
                uid="home-1",
                when=datetime(2025, 10, 24, 8, 0, tzinfo=timezone.utc),
                amount_minor_units=-3539,
                counterparty="Global Organic Markets Stroud",
                category="Groceries",
                raw_json=_raw_payload(country="GB"),
            ),
            _make_spend(
                uid="home-2",
                when=datetime(2025, 10, 24, 9, 0, tzinfo=timezone.utc),
                amount_minor_units=-860,
                counterparty="Golden Sheep Coffee",
                category="Groceries",
                raw_json=_raw_payload(country="GB"),
            ),
            _make_spend(
                uid="home-3",
                when=datetime(2025, 10, 24, 12, 0, tzinfo=timezone.utc),
                amount_minor_units=-500,
                counterparty="The Stroud Hotel",
                category="Eating Out",
                raw_json=_raw_payload(country="GB"),
            ),
        ]
    )

    payload = calculate_holiday_signals(
        days=30,
        reference_time=datetime(2025, 10, 31, tzinfo=timezone.utc),
    )

    assert payload["summary"]["clustersCount"] == 0


def test_holiday_signals_excludes_frequent_home_merchant_from_domestic_trip():
    historical_waitrose_spends = [
        _make_spend(
            uid=f"hist-{index}",
            when=datetime(2025, 1, index + 1, 12, 0, tzinfo=timezone.utc),
            amount_minor_units=-2500,
            counterparty="Waitrose & Partners",
            category="Groceries",
            raw_json=_raw_payload(country="GB"),
        )
        for index in range(6)
    ]

    FeedItem.objects.bulk_create(
        historical_waitrose_spends
        + [
            _make_spend(
                uid="anchor-1",
                when=datetime(2025, 4, 23, 18, 0, tzinfo=timezone.utc),
                amount_minor_units=-27598,
                counterparty="Airbnb",
                category="Holidays",
                raw_json=_raw_payload(country="GB", reference="AIRBNB * HMTEST123"),
            ),
            _make_spend(
                uid="trip-restaurant",
                when=datetime(2025, 4, 24, 19, 0, tzinfo=timezone.utc),
                amount_minor_units=-8250,
                counterparty="Wal Bach",
                category="Eating Out",
                raw_json=_raw_payload(country="GB"),
            ),
            _make_spend(
                uid="trip-groceries",
                when=datetime(2025, 4, 24, 10, 0, tzinfo=timezone.utc),
                amount_minor_units=-3120,
                counterparty="Waitrose & Partners",
                category="Groceries",
                raw_json=_raw_payload(country="GB"),
            ),
        ]
    )

    payload = calculate_holiday_signals(
        days=180,
        reference_time=datetime(2025, 5, 31, tzinfo=timezone.utc),
    )

    assert payload["summary"]["clustersCount"] == 1
    cluster = payload["clusters"][0]
    assert {item["feedItemUid"] for item in cluster["transactions"]} == {
        "anchor-1",
        "trip-restaurant",
    }
    assert {item["feedItemUid"] for item in cluster["transactions"] if item["suggested"]} == {
        "anchor-1",
        "trip-restaurant",
    }


def test_holiday_signals_excludes_foreign_subscription_noise():
    FeedItem.objects.bulk_create(
        [
            _make_spend(
                uid="mx-trip-1",
                when=datetime(2025, 8, 1, 10, 0, tzinfo=timezone.utc),
                amount_minor_units=-3003,
                counterparty="Clip Mx*vegan Barrio",
                category="Eating Out",
                raw_json=_raw_payload(country="MX", source_currency="MXN", minor_units=74250),
            ),
            _make_spend(
                uid="mx-trip-2",
                when=datetime(2025, 8, 1, 12, 0, tzinfo=timezone.utc),
                amount_minor_units=-5316,
                counterparty="Mega Cozumel",
                category="Groceries",
                raw_json=_raw_payload(country="MX", source_currency="MXN", minor_units=131500),
            ),
            _make_spend(
                uid="mx-trip-3",
                when=datetime(2025, 8, 1, 18, 0, tzinfo=timezone.utc),
                amount_minor_units=-9414,
                counterparty="Cash Machine (Mexico)",
                category="Bills And Services",
                raw_json=_raw_payload(country="MX", source_currency="MXN", minor_units=232900),
            ),
            _make_spend(
                uid="mx-noise-1",
                when=datetime(2025, 8, 1, 20, 0, tzinfo=timezone.utc),
                amount_minor_units=-899,
                counterparty="Apple App Store",
                category="Subscriptions",
                raw_json=_raw_payload(country="MX", source_currency="MXN", minor_units=22300),
            ),
        ]
    )

    payload = calculate_holiday_signals(
        days=60,
        reference_time=datetime(2025, 8, 31, tzinfo=timezone.utc),
    )

    assert payload["summary"]["clustersCount"] == 1
    cluster = payload["clusters"][0]
    assert {item["feedItemUid"] for item in cluster["transactions"]} == {
        "mx-trip-1",
        "mx-trip-2",
        "mx-trip-3",
    }


def test_holiday_signals_excludes_routine_travel_edges_from_foreign_trip():
    historical_edges = []
    for index in range(4):
        historical_edges.extend(
            [
                _make_spend(
                    uid=f"hist-mipermit-{index}",
                    when=datetime(2025, 2, index + 1, 9, 0, tzinfo=timezone.utc),
                    amount_minor_units=-850,
                    counterparty="MiPermit",
                    category="Transport",
                    raw_json=_raw_payload(country="GB"),
                ),
                _make_spend(
                    uid=f"hist-gwr-{index}",
                    when=datetime(2025, 3, index + 1, 18, 0, tzinfo=timezone.utc),
                    amount_minor_units=-4200,
                    counterparty="Great Western Railway",
                    category="Expenses",
                    raw_json=_raw_payload(country="GB"),
                ),
            ]
        )

    FeedItem.objects.bulk_create(
        historical_edges
        + [
            _make_spend(
                uid="edge-before",
                when=datetime(2025, 7, 31, 8, 0, tzinfo=timezone.utc),
                amount_minor_units=-1200,
                counterparty="MiPermit",
                category="Transport",
                raw_json=_raw_payload(country="GB"),
            ),
            _make_spend(
                uid="foreign-1",
                when=datetime(2025, 8, 1, 10, 0, tzinfo=timezone.utc),
                amount_minor_units=-3003,
                counterparty="Clip Mx*vegan Barrio",
                category="Eating Out",
                raw_json=_raw_payload(country="MX", source_currency="MXN", minor_units=74250),
            ),
            _make_spend(
                uid="foreign-2",
                when=datetime(2025, 8, 2, 12, 0, tzinfo=timezone.utc),
                amount_minor_units=-5316,
                counterparty="Mega Cozumel",
                category="Groceries",
                raw_json=_raw_payload(country="MX", source_currency="MXN", minor_units=131500),
            ),
            _make_spend(
                uid="edge-after",
                when=datetime(2025, 8, 3, 18, 0, tzinfo=timezone.utc),
                amount_minor_units=-4200,
                counterparty="Great Western Railway",
                category="Expenses",
                raw_json=_raw_payload(country="GB"),
            ),
        ]
    )

    payload = calculate_holiday_signals(
        days=365,
        reference_time=datetime(2025, 8, 31, tzinfo=timezone.utc),
    )

    assert payload["summary"]["clustersCount"] == 1
    cluster = payload["clusters"][0]
    assert {item["feedItemUid"] for item in cluster["transactions"]} == {
        "foreign-1",
        "foreign-2",
    }


def test_holiday_signals_respects_review_decisions_and_anchor_override():
    FeedItem.objects.bulk_create(
        [
            _make_spend(
                uid="decision-1",
                when=datetime(2025, 4, 23, 18, 0, tzinfo=timezone.utc),
                amount_minor_units=-27598,
                counterparty="Airbnb",
                category="Holidays",
                raw_json=_raw_payload(country="GB", reference="AIRBNB * HMTEST123"),
            ),
            _make_spend(
                uid="decision-2",
                when=datetime(2025, 4, 24, 19, 0, tzinfo=timezone.utc),
                amount_minor_units=-8250,
                counterparty="Wal Bach",
                category="Eating Out",
                raw_json=_raw_payload(country="GB"),
            ),
            _make_spend(
                uid="anchor-override-1",
                when=datetime(2025, 9, 15, 16, 0, tzinfo=timezone.utc),
                amount_minor_units=-6400,
                counterparty="Odd Merchant",
                category="Bills And Services",
                raw_json=_raw_payload(country="GB"),
            ),
        ]
    )
    HolidaySuggestionDecision.objects.create(feed_item_uid="decision-2", decision="rejected")
    HolidayMerchantOverride.objects.create(
        merchant_key="odd merchant",
        label="Odd Merchant",
        override_type="holiday_anchor",
    )

    payload = calculate_holiday_signals(
        days=365,
        reference_time=datetime(2025, 10, 31, tzinfo=timezone.utc),
    )

    assert payload["summary"]["clustersCount"] == 2
    april_cluster = next(cluster for cluster in payload["clusters"] if cluster["start"].startswith("2025-04-23"))
    september_cluster = next(cluster for cluster in payload["clusters"] if cluster["start"].startswith("2025-09-15"))
    assert {item["feedItemUid"] for item in april_cluster["transactions"]} == {"decision-1", "decision-2"}
    rejected_item = next(item for item in april_cluster["transactions"] if item["feedItemUid"] == "decision-2")
    assert rejected_item["reviewDecision"] == "rejected"
    assert rejected_item["suggested"] is False
    assert {item["feedItemUid"] for item in september_cluster["transactions"]} == {"anchor-override-1"}
    assert "holiday_anchor_override" in september_cluster["transactions"][0]["reasonCodes"]


def test_holiday_signals_hides_accepted_items_unless_requested():
    FeedItem.objects.bulk_create(
        [
            _make_spend(
                uid="accepted-1",
                when=datetime(2025, 4, 23, 18, 0, tzinfo=timezone.utc),
                amount_minor_units=-27598,
                counterparty="Airbnb",
                category="Holidays",
                raw_json=_raw_payload(country="GB", reference="AIRBNB * HMTEST123"),
            ),
            _make_spend(
                uid="accepted-2",
                when=datetime(2025, 4, 24, 19, 0, tzinfo=timezone.utc),
                amount_minor_units=-8250,
                counterparty="Wal Bach",
                category="Eating Out",
                raw_json=_raw_payload(country="GB"),
            ),
        ]
    )
    HolidaySuggestionDecision.objects.create(feed_item_uid="accepted-1", decision="accepted")
    HolidaySuggestionDecision.objects.create(feed_item_uid="accepted-2", decision="accepted")

    hidden_payload = calculate_holiday_signals(
        days=365,
        reference_time=datetime(2025, 10, 31, tzinfo=timezone.utc),
    )
    shown_payload = calculate_holiday_signals(
        days=365,
        reference_time=datetime(2025, 10, 31, tzinfo=timezone.utc),
        include_reviewed=True,
    )

    assert hidden_payload["summary"]["clustersCount"] == 0
    assert shown_payload["summary"]["clustersCount"] == 1
    shown_cluster = shown_payload["clusters"][0]
    assert shown_cluster["suggestedTransactionCount"] == 0
    assert {item["feedItemUid"] for item in shown_cluster["transactions"]} == {"accepted-1", "accepted-2"}
