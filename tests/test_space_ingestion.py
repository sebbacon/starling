from __future__ import annotations

from datetime import datetime, timezone

import pytest
import respx

from starling_spaces.ingestion import calculate_average_spend, sync_space_feeds
from starling_web.spaces.models import Category, FeedItem, SyncState

pytestmark = pytest.mark.django_db

@respx.mock
def test_sync_space_feeds_persists_feed_items(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={
            "accounts": [
                {
                    "accountUid": "acc-123",
                    "name": "Personal",
                    "currency": "GBP",
                    "defaultCategory": "cat-123",
                }
            ]
        }
    )
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/accounts/acc-123/balance"
    ).respond(
        json={
            "effectiveBalance": {
                "currency": "GBP",
                "minorUnits": 750000,
            }
        }
    )
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/account/acc-123/spaces"
    ).respond(
        json={
            "spaceList": [
                {
                    "spaceUid": "space-1",
                    "name": "Rainy Day",
                    "totalSaved": {"currency": "GBP", "minorUnits": 120000},
                    "goalAmount": {"currency": "GBP", "minorUnits": 200000},
                    "settings": {},
                },
                {
                    "spaceUid": "space-2",
                    "name": "Holiday",
                    "totalSaved": {"currency": "GBP", "minorUnits": 3050},
                    "settings": {},
                },
            ]
        }
    )
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/account/acc-123/savings-goals/space-1/recurring-transfer"
    ).respond(status_code=404, json={"error": "not found"})
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/account/acc-123/savings-goals/space-2/recurring-transfer"
    ).respond(status_code=404, json={"error": "not found"})

    feed_response_space_1 = {
        "feedItems": [
            {
                "feedItemUid": "feed-1",
                "categoryUid": "space-1",
                "transactionTime": "2024-11-10T10:00:00Z",
                "amount": {"currency": "GBP", "minorUnits": 2000},
                "direction": "OUT",
                "source": "CARD",
                "counterPartyName": "Coffee Shop",
                "spendingCategory": "SHOPPING",
            },
            {
                "feedItemUid": "feed-2",
                "categoryUid": "space-1",
                "transactionTime": "2024-09-01T09:00:00Z",
                "amount": {"currency": "GBP", "minorUnits": 1500},
                "direction": "OUT",
                "source": "CARD",
                "spendingCategory": "GROCERIES",
            },
            {
                "feedItemUid": "feed-3",
                "categoryUid": "space-1",
                "transactionTime": "2024-11-05T13:30:00Z",
                "amount": {"currency": "GBP", "minorUnits": 500},
                "direction": "IN",
                "source": "TRANSFER",
                "spendingCategory": "SAVING",
            },
        ],
        "pageable": {"next": None},
    }

    respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-123/category/space-1"
    ).respond(json=feed_response_space_1)

    respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-123/category/space-2"
    ).respond(
        json={
            "feedItems": [
                {
                    "feedItemUid": "feed-4",
                    "categoryUid": "space-2",
                    "transactionTime": "2024-11-12T18:45:00Z",
                    "amount": {"currency": "GBP", "minorUnits": 1000},
                    "direction": "OUT",
                    "source": "CARD",
                    "spendingCategory": "ENTERTAINMENT",
                }
            ],
            "pageable": {"next": None},
        }
    )

    sync_space_feeds("TOKEN")

    rows = list(
        FeedItem.objects.order_by("feed_item_uid").values(
            "feed_item_uid",
            "amount_minor_units",
            "transaction_time",
            "spending_category",
            "classified_category",
            "classification_reason",
        )
    )
    assert [row["feed_item_uid"] for row in rows] == [
        "feed-1",
        "feed-2",
        "feed-3",
        "feed-4",
    ]
    assert rows[0]["amount_minor_units"] == -2000
    assert rows[2]["amount_minor_units"] == 500
    assert rows[0]["spending_category"] == "SHOPPING"
    assert rows[3]["spending_category"] == "ENTERTAINMENT"
    assert rows[0]["classified_category"] == "Shopping"
    assert rows[0]["classification_reason"] == "starling_fallback"

    sync_state = SyncState.objects.get(account_uid="acc-123", category_uid="space-1")
    assert sync_state.last_transaction_time == "2024-11-10T10:00:00+00:00"

    categories = list(
        Category.objects.order_by("category_type", "category_uid").values(
            "category_type", "category_uid", "space_uid", "name"
        )
    )
    assert [
        (row["category_type"], row["category_uid"]) for row in categories
    ] == [
        ("space", "space-1"),
        ("space", "space-2"),
        ("spending", "ENTERTAINMENT"),
        ("spending", "GROCERIES"),
        ("spending", "SAVING"),
        ("spending", "SHOPPING"),
    ]
    first_space = categories[0]
    assert first_space["space_uid"] == "space-1"
    assert first_space["name"] == "Rainy Day"

    summary = calculate_average_spend(
        days=30,
        reference_time=datetime(2024, 11, 15, 0, 0, tzinfo=timezone.utc),
    )

    summary_by_space = {item["spaceUid"]: item for item in summary["spaces"]}
    assert summary_by_space["space-1"]["totalOutflowMinorUnits"] == 2000
    assert summary_by_space["space-1"]["averageDailySpendMinorUnits"] == 67
    assert summary_by_space["space-2"]["totalOutflowMinorUnits"] == 1000
    assert summary_by_space["space-2"]["averageDailySpendMinorUnits"] == 33

    spending = {item["category"]: item for item in summary["spendingCategories"]}
    assert spending["SHOPPING"]["outflowCount"] == 1
    assert spending["ENTERTAINMENT"]["totalOutflowMinorUnits"] == 1000
