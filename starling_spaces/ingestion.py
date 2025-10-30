from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from .reporting import (API_BASE_URL, RETRYABLE_STATUS_CODES, AccountReport,
                        Space, StarlingSchemaError, _parse_money,
                        _request_json, fetch_spaces_configuration)

DEFAULT_DB_PATH = Path("data/starling_feeds.db")
DEFAULT_CHANGES_SINCE = "2018-01-01T00:00:00Z"
ASSUMED_CURRENCY = "GBP"


@dataclass(frozen=True)
class FeedRecord:
    feed_item_uid: str
    account_uid: str
    category_uid: str
    space_uid: str
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
    db_path: Path | str = DEFAULT_DB_PATH,
    base_url: str = API_BASE_URL,
    timeout: float = 10.0,
    changes_since: Optional[str] = None,
    max_pages: Optional[int] = None,
) -> None:
    database = Path(db_path)
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        _ensure_schema(conn)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        with httpx.Client(base_url=base_url, headers=headers, timeout=timeout) as client:
            reports = fetch_spaces_configuration(token, base_url=base_url, timeout=timeout)
            for report in reports:
                _sync_account_spaces(
                    conn,
                    client,
                    report,
                    changes_since=changes_since,
                    max_pages=max_pages,
                )
        conn.commit()


def calculate_average_spend(
    *,
    db_path: Path | str = DEFAULT_DB_PATH,
    days: int = 30,
    reference_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    database = Path(db_path)
    reference = reference_time or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    start = reference - timedelta(days=days)
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                c.account_uid,
                c.category_uid AS space_uid,
                c.name,
                COALESCE(
                    SUM(CASE WHEN fi.amount_minor_units < 0 THEN -fi.amount_minor_units ELSE 0 END),
                    0
                ) AS total_outflow_minor,
                COALESCE(
                    SUM(CASE WHEN fi.amount_minor_units < 0 THEN 1 ELSE 0 END),
                    0
                ) AS outflow_count
            FROM categories c
            LEFT JOIN feed_items fi
              ON fi.account_uid = c.account_uid
             AND fi.space_uid = c.category_uid
             AND fi.transaction_time >= ?
             AND fi.transaction_time < ?
            WHERE c.category_type = 'space'
            GROUP BY c.account_uid, c.category_uid, c.name
            ORDER BY c.account_uid, c.category_uid
            """,
            (start.isoformat(), reference.isoformat()),
        ).fetchall()

    spaces: List[Dict[str, Any]] = []
    for row in rows:
        total_outflow = int(row["total_outflow_minor"])
        avg_minor = _average_minor_units(total_outflow, days)
        spaces.append(
            {
                "accountUid": row["account_uid"],
                "spaceUid": row["space_uid"],
                "spaceName": row["name"],
                "currency": ASSUMED_CURRENCY,
                "days": days,
                "totalOutflowMinorUnits": total_outflow,
                "totalOutflowFormatted": _format_minor_units(
                    ASSUMED_CURRENCY, total_outflow
                ),
                "averageDailySpendMinorUnits": avg_minor,
                "averageDailySpendFormatted": _format_minor_units(
                    ASSUMED_CURRENCY, avg_minor
                ),
                "outflowCount": int(row["outflow_count"]),
            }
        )
    return {"spaces": spaces}


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


def _sync_account_spaces(
    conn: sqlite3.Connection,
    client: httpx.Client,
    report: AccountReport,
    *,
    changes_since: Optional[str],
    max_pages: Optional[int],
) -> None:
    for space in report.spaces:
        category_uid = _space_category_uid(space)
        _upsert_category(
            conn,
            account_uid=report.account_uid,
            category_uid=category_uid,
            category_type="space",
            name=space.name,
            space_uid=space.uid,
        )
        _sync_space_feed(
            conn,
            client,
            report,
            space,
            category_uid,
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
    conn: sqlite3.Connection,
    account_uid: str,
    category_uid: str,
    *,
    category_type: str,
    name: Optional[str],
    space_uid: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO categories (
            account_uid,
            category_type,
            category_uid,
            space_uid,
            name
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(account_uid, category_type, category_uid) DO UPDATE SET
            space_uid = excluded.space_uid,
            name = excluded.name
        """,
        (
            account_uid,
            category_type,
            category_uid,
            space_uid,
            name,
        ),
    )


def _sync_space_feed(
    conn: sqlite3.Connection,
    client: httpx.Client,
    report: AccountReport,
    space: Space,
    category_uid: str,
    *,
    changes_since: Optional[str],
    max_pages: Optional[int],
) -> None:
    latest = _current_sync_cursor(conn, report.account_uid, category_uid)
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
        for raw_item in feed_items:
            record = _parse_feed_record(
                raw_item,
                account_uid=report.account_uid,
                category_uid=category_uid,
                space_uid=space.uid,
                currency_hint=report.currency,
            )
            _insert_feed_record(conn, record)
            if record.spending_category:
                _upsert_category(
                    conn,
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
            conn,
            report.account_uid,
            category_uid,
            cursor_time=newest_timestamp.isoformat(),
        )


def _parse_feed_record(
    raw: Dict[str, Any],
    *,
    account_uid: str,
    category_uid: str,
    space_uid: str,
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


def _insert_feed_record(conn: sqlite3.Connection, record: FeedRecord) -> None:
    conn.execute(
        """
        INSERT INTO feed_items (
            feed_item_uid,
            account_uid,
            category_uid,
            space_uid,
            direction,
            amount_minor_units,
            currency,
            transaction_time,
            source,
            counterparty,
            spending_category,
            raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(feed_item_uid) DO NOTHING
        """,
        (
            record.feed_item_uid,
            record.account_uid,
            record.category_uid,
            record.space_uid,
            record.direction,
            record.amount_minor_units,
            record.currency,
            record.transaction_time.isoformat(),
            record.source,
            record.counterparty,
            record.spending_category,
            json.dumps(record.raw, sort_keys=True),
        ),
    )


def _current_sync_cursor(
    conn: sqlite3.Connection,
    account_uid: str,
    category_uid: str,
) -> Optional[str]:
    row = conn.execute(
        "SELECT last_transaction_time FROM sync_state WHERE account_uid = ? AND category_uid = ?",
        (account_uid, category_uid),
    ).fetchone()
    if row:
        return row[0]
    return None


def _update_sync_state(
    conn: sqlite3.Connection,
    account_uid: str,
    category_uid: str,
    *,
    cursor_time: str,
) -> None:
    conn.execute(
        """
        INSERT INTO sync_state (account_uid, category_uid, last_transaction_time)
        VALUES (?, ?, ?)
        ON CONFLICT(account_uid, category_uid) DO UPDATE SET
            last_transaction_time = excluded.last_transaction_time
        """,
        (account_uid, category_uid, cursor_time),
    )


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feed_items (
            feed_item_uid TEXT PRIMARY KEY,
            account_uid TEXT NOT NULL,
            category_uid TEXT NOT NULL,
            space_uid TEXT NOT NULL,
            direction TEXT,
            amount_minor_units INTEGER NOT NULL,
            currency TEXT NOT NULL,
            transaction_time TEXT NOT NULL,
            source TEXT,
            counterparty TEXT,
            spending_category TEXT,
            raw_json TEXT NOT NULL
        )
        """
    )
    _ensure_column(
        conn,
        "feed_items",
        "spending_category",
        "ALTER TABLE feed_items ADD COLUMN spending_category TEXT",
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            account_uid TEXT NOT NULL,
            category_type TEXT NOT NULL,
            category_uid TEXT NOT NULL,
            space_uid TEXT,
            name TEXT,
            PRIMARY KEY (account_uid, category_type, category_uid)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_feed_items_account_space_time
            ON feed_items (account_uid, space_uid, transaction_time)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            account_uid TEXT NOT NULL,
            category_uid TEXT NOT NULL,
            last_transaction_time TEXT,
            PRIMARY KEY (account_uid, category_uid)
        )
        """
    )


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    existing = {
        row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column not in existing:
        conn.execute(ddl)


def _title_case_category(value: str) -> str:
    if not value:
        return value
    return value.replace("_", " ").title()
