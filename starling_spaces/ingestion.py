from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

import httpx
from django.db import transaction
from django.db.models import Count, F, Sum

from starling_web.spaces.models import Category, FeedItem, SyncState

from .classification import classify_for_storage
from .reporting import (
    API_BASE_URL,
    RETRYABLE_STATUS_CODES,
    AccountReport,
    Space,
    StarlingSchemaError,
    _parse_money,
    _request_json,
    fetch_spaces_configuration,
)

DEFAULT_CHANGES_SINCE = "2018-01-01T00:00:00Z"
ASSUMED_CURRENCY = "GBP"


@dataclass(frozen=True)
class FeedRecord:
    feed_item_uid: str
    account_uid: str
    category_uid: str
    space_uid: Optional[str]
    amount_minor_units: int
    currency: str
    direction: Optional[str]
    transaction_time: datetime
    source: Optional[str]
    counterparty: Optional[str]
    spending_category: Optional[str]
    raw: Dict[str, Any]


def sync_space_feeds(
    token: str,
    *,
    base_url: str = API_BASE_URL,
    timeout: float = 10.0,
    changes_since: Optional[str] = None,
    max_pages: Optional[int] = None,
) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    with httpx.Client(base_url=base_url, headers=headers, timeout=timeout) as client:
        reports = fetch_spaces_configuration(token, base_url=base_url, timeout=timeout)
        for report in reports:
            _sync_account_spaces(
                client,
                report,
                changes_since=changes_since,
                max_pages=max_pages,
            )


def calculate_average_spend(
    *,
    days: int = 30,
    reference_time: Optional[datetime] = None,
    account_balances: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")

    reference = reference_time or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    start = reference - timedelta(days=days)

    space_names = {
        (cat.account_uid, cat.category_uid): cat.name
        for cat in Category.objects.filter(category_type="space")
    }

    spending_names = {
        (cat.account_uid, cat.category_uid): cat.name
        for cat in Category.objects.filter(category_type="spending")
    }

    space_totals = {
        (row["account_uid"], row["space_uid"]): {
            "total": int(row["total_outflow_minor"] or 0),
            "count": int(row["outflow_count"] or 0),
        }
        for row in (
            FeedItem.objects.filter(
                transaction_time__gte=start,
                transaction_time__lt=reference,
                amount_minor_units__lt=0,
            )
            .values("account_uid", "space_uid")
            .annotate(
                total_outflow_minor=Sum(-F("amount_minor_units")),
                outflow_count=Count("feed_item_uid"),
            )
        )
    }

    spaces: List[Dict[str, Any]] = []
    for (account_uid, category_uid), name in space_names.items():
        stats = space_totals.get((account_uid, category_uid), {"total": 0, "count": 0})
        total_outflow = stats["total"]
        avg_minor = _average_minor_units(total_outflow, days)
        spaces.append(
            {
                "accountUid": account_uid,
                "spaceUid": category_uid,
                "spaceName": name,
                "currency": ASSUMED_CURRENCY,
                "days": days,
                "totalOutflowMinorUnits": total_outflow,
                "totalOutflowFormatted": _format_minor_units(ASSUMED_CURRENCY, total_outflow),
                "averageDailySpendMinorUnits": avg_minor,
                "averageDailySpendFormatted": _format_minor_units(ASSUMED_CURRENCY, avg_minor),
                "outflowCount": stats["count"],
            }
        )

    category_totals = {
        (row["account_uid"], row["spending_category"]): {
            "total": int(row["total_outflow_minor"] or 0),
            "count": int(row["outflow_count"] or 0),
        }
        for row in (
            FeedItem.objects.filter(
                transaction_time__gte=start,
                transaction_time__lt=reference,
                amount_minor_units__lt=0,
                spending_category__isnull=False,
            )
            .values("account_uid", "spending_category")
            .annotate(
                total_outflow_minor=Sum(-F("amount_minor_units")),
                outflow_count=Count("feed_item_uid"),
            )
        )
    }

    spending_categories: List[Dict[str, Any]] = []
    for (account_uid, category_uid), name in spending_names.items():
        stats = category_totals.get((account_uid, category_uid), {"total": 0, "count": 0})
        total_outflow = stats["total"]
        avg_minor = _average_minor_units(total_outflow, days)
        spending_categories.append(
            {
                "accountUid": account_uid,
                "category": category_uid,
                "name": name,
                "currency": ASSUMED_CURRENCY,
                "days": days,
                "totalOutflowMinorUnits": total_outflow,
                "totalOutflowFormatted": _format_minor_units(ASSUMED_CURRENCY, total_outflow),
                "averageDailySpendMinorUnits": avg_minor,
                "averageDailySpendFormatted": _format_minor_units(ASSUMED_CURRENCY, avg_minor),
                "outflowCount": stats["count"],
            }
        )

    result: Dict[str, Any] = {
        "spaces": spaces,
        "spendingCategories": spending_categories,
    }

    if account_balances is not None:
        result["accountBalances"] = account_balances

    return result


def _average_minor_units(total_outflow: int, days: int) -> int:
    if total_outflow <= 0:
        return 0
    from decimal import ROUND_HALF_UP, Decimal

    average = (Decimal(total_outflow) / Decimal(days)).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    return int(average)


def _format_minor_units(currency: str, minor_units: int) -> str:
    amount = minor_units / 100
    return f"{currency} {amount:,.2f}"


def fetch_account_balances(
    token: str,
    account_uids: Sequence[str],
    *,
    base_url: str = API_BASE_URL,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    unique = sorted({uid for uid in account_uids if uid})
    if not unique:
        return {}
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    balances: Dict[str, Any] = {}
    with httpx.Client(base_url=base_url, headers=headers, timeout=timeout) as client:
        for account_uid in unique:
            data = _request_json(
                client,
                "GET",
                f"/api/v2/accounts/{account_uid}/balance",
                max_attempts=3,
                retry_statuses=RETRYABLE_STATUS_CODES,
            )
            money = _parse_money(
                data.get("effectiveBalance"), default_currency=ASSUMED_CURRENCY
            )
            if not money:
                money = _parse_money(
                    data.get("clearedBalance"), default_currency=ASSUMED_CURRENCY
                )
            if money:
                currency = money.currency
                minor_units = money.minor_units
            else:
                currency = ASSUMED_CURRENCY
                minor_units = 0
            balances[account_uid] = {
                "currency": currency,
                "minorUnits": minor_units,
                "formatted": _format_minor_units(currency, minor_units),
                "raw": data,
            }
    return balances


def _sync_account_spaces(
    client: httpx.Client,
    report: AccountReport,
    *,
    changes_since: Optional[str],
    max_pages: Optional[int],
) -> None:
    default_category_uid = report.default_category
    if default_category_uid:
        default_name = report.account_name or "Account"
        _upsert_category(
            account_uid=report.account_uid,
            category_uid=default_category_uid,
            category_type="account",
            name=default_name,
            space_uid=None,
        )
        _sync_feed_category(
            client,
            report,
            category_uid=default_category_uid,
            space_uid=None,
            space_name=default_name,
            changes_since=changes_since,
            max_pages=max_pages,
        )

    for space in report.spaces:
        category_uid = _space_category_uid(space)
        _upsert_category(
            account_uid=report.account_uid,
            category_uid=category_uid,
            category_type="space",
            name=space.name,
            space_uid=space.uid,
        )
        _sync_feed_category(
            client,
            report,
            category_uid=category_uid,
            space_uid=space.uid,
            space_name=space.name,
            changes_since=changes_since,
            max_pages=max_pages,
        )


def _space_category_uid(space: Space) -> str:
    raw = space.raw or {}
    for key in (
        "categoryUid",
        "spaceCategoryUid",
        "savingsGoalCategoryUid",
        "feedCategoryUid",
    ):
        value = raw.get(key)
        if value:
            return str(value)
    if space.uid:
        return space.uid
    raise StarlingSchemaError(f"Space missing UID for category resolution: {space}")


def _upsert_category(
    *,
    account_uid: str,
    category_uid: str,
    category_type: str,
    name: Optional[str],
    space_uid: Optional[str] = None,
) -> None:
    Category.objects.update_or_create(
        account_uid=account_uid,
        category_type=category_type,
        category_uid=category_uid,
        defaults={"space_uid": space_uid, "name": name},
    )


def _sync_feed_category(
    client: httpx.Client,
    report: AccountReport,
    category_uid: str,
    space_uid: Optional[str],
    space_name: Optional[str],
    *,
    changes_since: Optional[str],
    max_pages: Optional[int],
) -> None:
    latest = _current_sync_cursor(report.account_uid, category_uid)
    cursor = changes_since or latest or DEFAULT_CHANGES_SINCE
    url = f"/api/v2/feed/account/{report.account_uid}/category/{category_uid}"
    params: Optional[Dict[str, Any]] = {"changesSince": cursor}
    pages_fetched = 0
    newest_timestamp: Optional[datetime] = None
    next_url: Optional[str] = None

    while True:
        pages_fetched += 1
        data = _request_json(
            client,
            "GET",
            next_url or url,
            params=params if next_url is None else None,
            max_attempts=3,
            retry_statuses=RETRYABLE_STATUS_CODES,
        )
        feed_items = data.get("feedItems") or []
        with transaction.atomic():
            for raw_item in feed_items:
                record = _parse_feed_record(
                    raw_item,
                    account_uid=report.account_uid,
                    category_uid=category_uid,
                    space_uid=space_uid,
                    currency_hint=report.currency,
                )
                classification = classify_for_storage(record, space_name)
                _insert_feed_record(record, classification)
                if record.spending_category:
                    _upsert_category(
                        account_uid=report.account_uid,
                        category_uid=record.spending_category,
                        category_type="spending",
                        name=_title_case_category(record.spending_category),
                    )
                if newest_timestamp is None or record.transaction_time > newest_timestamp:
                    newest_timestamp = record.transaction_time

        pageable = data.get("pageable") or {}
        next_url = pageable.get("next")
        params = None
        if max_pages is not None and pages_fetched >= max_pages:
            break
        if not next_url:
            break

    if newest_timestamp:
        _update_sync_state(
            report.account_uid,
            category_uid,
            cursor_time=newest_timestamp.isoformat(),
        )


def _parse_feed_record(
    raw: Dict[str, Any],
    *,
    account_uid: str,
    category_uid: str,
    space_uid: Optional[str],
    currency_hint: Optional[str],
) -> FeedRecord:
    feed_item_uid = raw.get("feedItemUid")
    if not feed_item_uid:
        raise StarlingSchemaError("Feed item missing feedItemUid")

    money = _extract_feed_money(raw, currency_hint)
    direction = raw.get("direction")
    amount_minor_units = _normalise_minor_units(money.minor_units, direction)

    timestamp = _extract_feed_timestamp(raw)
    counterparty = raw.get("counterPartyName") or raw.get("counterPartyType")
    source = raw.get("source")
    spending_category = raw.get("spendingCategory")

    return FeedRecord(
        feed_item_uid=str(feed_item_uid),
        account_uid=account_uid,
        category_uid=category_uid,
        space_uid=space_uid,
        amount_minor_units=amount_minor_units,
        currency=money.currency,
        direction=str(direction) if direction else None,
        transaction_time=timestamp,
        source=str(source) if source else None,
        counterparty=str(counterparty) if counterparty else None,
        spending_category=str(spending_category) if spending_category else None,
        raw=raw,
    )


def _extract_feed_money(raw: Dict[str, Any], currency_hint: Optional[str]):
    for key in ("amount", "sourceAmount", "totalAmount", "accountAmount"):
        money = _parse_money(raw.get(key), default_currency=currency_hint)
        if money:
            return money
    raise StarlingSchemaError("Feed item missing recognised amount")


def _normalise_minor_units(minor_units: int, direction: Optional[str]) -> int:
    if not direction:
        return minor_units
    normalised = minor_units
    upper = direction.upper()
    if upper in {"OUT", "DEBIT"} and minor_units > 0:
        normalised = -minor_units
    elif upper in {"IN", "CREDIT"} and minor_units < 0:
        normalised = -minor_units
    return normalised


def _extract_feed_timestamp(raw: Dict[str, Any]) -> datetime:
    value: Optional[str] = None
    for key in (
        "transactionTime",
        "transactionTimestamp",
        "postedTimestamp",
        "settlementTimestamp",
    ):
        raw_value = raw.get(key)
        if raw_value:
            value = str(raw_value)
            break
    if not value:
        raise StarlingSchemaError("Feed item missing timestamp")
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    else:
        timestamp = timestamp.astimezone(timezone.utc)
    return timestamp


def _insert_feed_record(record: FeedRecord, classification) -> None:
    FeedItem.objects.update_or_create(
        feed_item_uid=record.feed_item_uid,
        defaults={
            "account_uid": record.account_uid,
            "category_uid": record.category_uid,
            "space_uid": record.space_uid,
            "direction": record.direction,
            "amount_minor_units": record.amount_minor_units,
            "currency": record.currency,
            "transaction_time": record.transaction_time,
            "source": record.source,
            "counterparty": record.counterparty,
            "spending_category": record.spending_category,
            "classified_category": classification.category,
            "classification_reason": classification.reason,
            "raw_json": record.raw,
        },
    )


def _current_sync_cursor(account_uid: str, category_uid: str) -> Optional[str]:
    try:
        state = SyncState.objects.get(account_uid=account_uid, category_uid=category_uid)
    except SyncState.DoesNotExist:
        return None
    return state.last_transaction_time


def _update_sync_state(account_uid: str, category_uid: str, *, cursor_time: str) -> None:
    SyncState.objects.update_or_create(
        account_uid=account_uid,
        category_uid=category_uid,
        defaults={"last_transaction_time": cursor_time},
    )


def _title_case_category(value: str) -> str:
    if not value:
        return value
    return value.replace("_", " ").title()
