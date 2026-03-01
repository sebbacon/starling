from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
from zoneinfo import ZoneInfo

from .reporting import (
    API_BASE_URL,
    RETRYABLE_STATUS_CODES,
    StarlingAPIError,
    _extract_accounts,
    _extract_error_message,
    _extract_spaces,
    _parse_money,
    _parse_space,
    _request_json,
    _retry_delay,
)

MAIN_ACCOUNT_NAME = "Joint"
SALARY_COUNTERPARTY = "University of Oxfo"
SALARY_MIN_MINOR_UNITS = 500000
SALARY_MAX_MINOR_UNITS = 600000
SALARY_LOOKBACK_DAYS = 62
SALARY_DRAWDOWN_SPACE = "Salary drawdown"

FIXED_ALLOCATIONS = (
    ("mortgage", "Mortgage (monthly)", 97000),
    ("groceries", "Groceries (monthly)", 80000),
    ("holidays", "Holidays", 40000),
)

TOP_UP_TARGETS = (
    ("bills", "Bills (monthly)", 110000),
    ("kids", "Kids (monthly)", 30000),
)

RELEASE_DAYS = (8, 15, 23)
LONDON_TZ = ZoneInfo("Europe/London")


class SalaryAutomationError(RuntimeError):
    """Raised when salary automation cannot safely proceed."""


@dataclass(frozen=True)
class SpaceSnapshot:
    uid: str
    name: str
    balance_minor_units: int
    currency: str


@dataclass(frozen=True)
class PlannedTransfer:
    leg: str
    direction: str
    space_name: str
    space_uid: str
    amount_minor_units: int


def split_into_three_tranches(total_minor_units: int) -> List[int]:
    if total_minor_units < 0:
        raise ValueError("total_minor_units must be non-negative")
    base = total_minor_units // 3
    remainder = total_minor_units % 3
    parts = [base, base, base]
    for index in range(remainder):
        parts[index] += 1
    return parts


def due_release_count(
    salary_time: datetime,
    *,
    now: Optional[datetime] = None,
) -> int:
    current = _as_utc(now or datetime.now(timezone.utc))
    salary = _as_utc(salary_time)
    if current <= salary:
        return 0

    current_local_date = current.astimezone(LONDON_TZ).date()
    salary_local_date = salary.astimezone(LONDON_TZ).date()
    day_offset = (current_local_date - salary_local_date).days
    cycle_day = day_offset + 1

    due = 0
    for day_threshold in RELEASE_DAYS:
        if cycle_day >= day_threshold:
            due += 1
    return due


def run_salary_automation(
    token: str,
    *,
    base_url: str = API_BASE_URL,
    timeout: float = 10.0,
    now: Optional[datetime] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    cleaned_token = token.strip() if token else ""
    if not cleaned_token:
        raise SalaryAutomationError("STARLING_PAT must not be blank")

    now_utc = _as_utc(now or datetime.now(timezone.utc))
    headers = {
        "Authorization": f"Bearer {cleaned_token}",
        "Accept": "application/json",
    }

    with httpx.Client(base_url=base_url, headers=headers, timeout=timeout) as client:
        account = _resolve_main_account(client)
        spaces = _resolve_required_spaces(
            client,
            account_uid=account["uid"],
            account_currency=account["currency"],
        )
        feed_items = _fetch_feed_items(
            client,
            account_uid=account["uid"],
            category_uid=account["default_category"],
            changes_since=_isoformat_utc(
                now_utc - timedelta(days=SALARY_LOOKBACK_DAYS)
            ),
        )
        salary = _find_latest_salary_payment(feed_items)
        if salary is None:
            return {
                "status": "no_salary_detected",
                "executed": 0,
                "alreadyDone": 0,
                "actions": [],
            }

        salary_minor_units = salary["minor_units"]
        salary_time = salary["timestamp"]
        cycle_id = salary["feed_item_uid"]
        currency = salary["currency"]

        allocations, drawdown_funding = _plan_initial_allocations(
            spaces=spaces,
            salary_minor_units=salary_minor_units,
        )
        due_tranches = due_release_count(salary_time, now=now_utc)
        tranche_minor_units = split_into_three_tranches(drawdown_funding)

        release_transfers: List[PlannedTransfer] = []
        for index, amount in enumerate(tranche_minor_units, start=2):
            release_transfers.append(
                PlannedTransfer(
                    leg=f"release_q{index}",
                    direction="to_main",
                    space_name=SALARY_DRAWDOWN_SPACE,
                    space_uid=spaces[SALARY_DRAWDOWN_SPACE].uid,
                    amount_minor_units=amount,
                )
            )

        planned = allocations + release_transfers[:due_tranches]

        actions: List[Dict[str, Any]] = []
        executed = 0
        already_done = 0

        for transfer in planned:
            if dry_run:
                result = "would_execute"
            else:
                result = _execute_planned_transfer(
                    client,
                    account_uid=account["uid"],
                    currency=currency,
                    cycle_id=cycle_id,
                    transfer=transfer,
                )
            actions.append(
                {
                    "leg": transfer.leg,
                    "direction": transfer.direction,
                    "space": transfer.space_name,
                    "amountMinorUnits": transfer.amount_minor_units,
                    "result": result,
                }
            )
            if result == "executed":
                executed += 1
            elif result == "already_done":
                already_done += 1

        for transfer in release_transfers[due_tranches:]:
            actions.append(
                {
                    "leg": transfer.leg,
                    "direction": transfer.direction,
                    "space": transfer.space_name,
                    "amountMinorUnits": transfer.amount_minor_units,
                    "result": "not_due",
                }
            )

        return {
            "status": "ok",
            "dryRun": dry_run,
            "cycleId": cycle_id,
            "salaryTimestamp": salary_time.isoformat(),
            "salaryMinorUnits": salary_minor_units,
            "drawdownFundingMinorUnits": drawdown_funding,
            "dueReleaseCount": due_tranches,
            "executed": executed,
            "alreadyDone": already_done,
            "actions": actions,
        }


def _resolve_main_account(client: httpx.Client) -> Dict[str, str]:
    accounts_data = _request_json(
        client,
        "GET",
        "/api/v2/accounts",
        max_attempts=3,
        retry_statuses=RETRYABLE_STATUS_CODES,
    )
    account_items = _extract_accounts(accounts_data)
    matching = [item for item in account_items if item.get("name") == MAIN_ACCOUNT_NAME]
    if not matching:
        raise SalaryAutomationError(f"Account not found: {MAIN_ACCOUNT_NAME}")
    if len(matching) > 1:
        raise SalaryAutomationError(
            f"Multiple accounts matched name: {MAIN_ACCOUNT_NAME}"
        )

    account = matching[0]
    account_uid = account.get("accountUid") or account.get("id")
    if not account_uid:
        raise SalaryAutomationError("Matched account is missing account UID")
    default_category = account.get("defaultCategory")
    if not default_category:
        raise SalaryAutomationError("Matched account is missing defaultCategory")
    currency = account.get("currency")
    if not currency:
        raise SalaryAutomationError("Matched account is missing currency")

    return {
        "uid": str(account_uid),
        "default_category": str(default_category),
        "currency": str(currency),
    }


def _resolve_required_spaces(
    client: httpx.Client,
    *,
    account_uid: str,
    account_currency: str,
) -> Dict[str, SpaceSnapshot]:
    spaces_data = _request_json(
        client,
        "GET",
        f"/api/v2/account/{account_uid}/spaces",
        max_attempts=3,
        retry_statuses=RETRYABLE_STATUS_CODES,
    )
    space_items = _extract_spaces(spaces_data)
    snapshots: Dict[str, SpaceSnapshot] = {}
    for item in space_items:
        parsed = _parse_space(item, account_currency=account_currency)
        if parsed.name in snapshots:
            raise SalaryAutomationError(f"Duplicate space name found: {parsed.name}")
        snapshots[parsed.name] = SpaceSnapshot(
            uid=parsed.uid,
            name=parsed.name,
            balance_minor_units=parsed.balance.minor_units,
            currency=parsed.balance.currency,
        )

    required_names = [name for _, name, _ in FIXED_ALLOCATIONS]
    required_names.extend(name for _, name, _ in TOP_UP_TARGETS)
    required_names.append(SALARY_DRAWDOWN_SPACE)

    missing = [name for name in required_names if name not in snapshots]
    if missing:
        raise SalaryAutomationError(
            f"Required spaces missing: {', '.join(sorted(missing))}"
        )

    return {name: snapshots[name] for name in required_names}


def _fetch_feed_items(
    client: httpx.Client,
    *,
    account_uid: str,
    category_uid: str,
    changes_since: str,
) -> List[Dict[str, Any]]:
    url = f"/api/v2/feed/account/{account_uid}/category/{category_uid}"
    params: Optional[Dict[str, Any]] = {"changesSince": changes_since}
    next_url: Optional[str] = None
    items: List[Dict[str, Any]] = []

    while True:
        data = _request_json(
            client,
            "GET",
            next_url or url,
            params=params if next_url is None else None,
            max_attempts=3,
            retry_statuses=RETRYABLE_STATUS_CODES,
        )
        feed_items = data.get("feedItems") or []
        for item in feed_items:
            if isinstance(item, dict):
                items.append(item)

        pageable = data.get("pageable") or {}
        next_url = pageable.get("next")
        params = None
        if not next_url:
            break

    return items


def _find_latest_salary_payment(
    feed_items: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    candidates = []
    for item in feed_items:
        if not _matches_salary_counterparty(item):
            continue
        money = _extract_feed_money(item)
        if money is None:
            continue
        amount_minor_units = _normalise_minor_units(
            money.minor_units,
            item.get("direction"),
        )
        if amount_minor_units < SALARY_MIN_MINOR_UNITS:
            continue
        if amount_minor_units > SALARY_MAX_MINOR_UNITS:
            continue
        feed_item_uid = item.get("feedItemUid")
        if not feed_item_uid:
            continue
        timestamp = _extract_timestamp(item)
        candidates.append(
            {
                "feed_item_uid": str(feed_item_uid),
                "timestamp": timestamp,
                "minor_units": amount_minor_units,
                "currency": money.currency,
            }
        )

    if not candidates:
        return None
    return max(candidates, key=lambda value: value["timestamp"])


def _matches_salary_counterparty(item: Dict[str, Any]) -> bool:
    counterparty = item.get("counterPartyName") or item.get("counterPartyType") or ""
    return SALARY_COUNTERPARTY.lower() in str(counterparty).lower()


def _extract_feed_money(item: Dict[str, Any]):
    for key in ("amount", "sourceAmount", "totalAmount", "accountAmount"):
        money = _parse_money(item.get(key))
        if money:
            return money
    return None


def _extract_timestamp(item: Dict[str, Any]) -> datetime:
    for key in (
        "transactionTime",
        "transactionTimestamp",
        "postedTimestamp",
        "settlementTimestamp",
    ):
        value = item.get(key)
        if not value:
            continue
        return _parse_timestamp(str(value))
    raise SalaryAutomationError("Salary candidate missing timestamp")


def _parse_timestamp(value: str) -> datetime:
    cleaned = value
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalise_minor_units(minor_units: int, direction: Optional[str]) -> int:
    if not direction:
        return minor_units
    upper = str(direction).upper()
    if upper in {"OUT", "DEBIT"} and minor_units > 0:
        return -minor_units
    if upper in {"IN", "CREDIT"} and minor_units < 0:
        return -minor_units
    return minor_units


def _plan_initial_allocations(
    *,
    spaces: Dict[str, SpaceSnapshot],
    salary_minor_units: int,
) -> tuple[List[PlannedTransfer], int]:
    planned: List[PlannedTransfer] = []
    initial_total = 0

    for key, space_name, amount_minor_units in FIXED_ALLOCATIONS:
        planned.append(
            PlannedTransfer(
                leg=f"allocation_{key}",
                direction="to_space",
                space_name=space_name,
                space_uid=spaces[space_name].uid,
                amount_minor_units=amount_minor_units,
            )
        )
        initial_total += amount_minor_units

    for key, space_name, target_minor_units in TOP_UP_TARGETS:
        top_up_minor_units = target_minor_units - spaces[space_name].balance_minor_units
        if top_up_minor_units < 0:
            top_up_minor_units = 0
        planned.append(
            PlannedTransfer(
                leg=f"allocation_{key}_topup",
                direction="to_space",
                space_name=space_name,
                space_uid=spaces[space_name].uid,
                amount_minor_units=top_up_minor_units,
            )
        )
        initial_total += top_up_minor_units

    remainder = salary_minor_units - initial_total
    if remainder < 0:
        raise SalaryAutomationError(
            "Salary amount is insufficient for fixed allocations and top-ups"
        )
    drawdown_funding = (remainder * 3) // 4
    planned.append(
        PlannedTransfer(
            leg="allocation_salary_drawdown",
            direction="to_space",
            space_name=SALARY_DRAWDOWN_SPACE,
            space_uid=spaces[SALARY_DRAWDOWN_SPACE].uid,
            amount_minor_units=drawdown_funding,
        )
    )

    return planned, drawdown_funding


def _execute_planned_transfer(
    client: httpx.Client,
    *,
    account_uid: str,
    currency: str,
    cycle_id: str,
    transfer: PlannedTransfer,
) -> str:
    if transfer.amount_minor_units <= 0:
        return "skipped_zero"

    transfer_uid = _transfer_uid(cycle_id, transfer.leg)
    if transfer.direction == "to_space":
        endpoint = (
            f"/api/v2/account/{account_uid}/savings-goals/{transfer.space_uid}/"
            f"add-money/{transfer_uid}"
        )
    elif transfer.direction == "to_main":
        endpoint = (
            f"/api/v2/account/{account_uid}/savings-goals/{transfer.space_uid}/"
            f"withdraw-money/{transfer_uid}"
        )
    else:
        raise SalaryAutomationError(f"Unknown transfer direction: {transfer.direction}")

    payload = {
        "amount": {
            "currency": currency,
            "minorUnits": transfer.amount_minor_units,
        }
    }

    try:
        _request_json_with_payload(
            client,
            "PUT",
            endpoint,
            payload=payload,
            max_attempts=3,
            retry_statuses=RETRYABLE_STATUS_CODES,
        )
    except StarlingAPIError as exc:
        if _is_duplicate_transfer_error(exc):
            return "already_done"
        raise

    return "executed"


def _request_json_with_payload(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    payload: Dict[str, Any],
    max_attempts: int = 1,
    retry_statuses: tuple[int, ...] = (),
) -> Dict[str, Any]:
    attempt = 0
    while True:
        try:
            response = client.request(method, url, json=payload)
        except httpx.HTTPError as exc:
            raise StarlingAPIError(f"Network error: {exc}") from exc

        if response.status_code in retry_statuses and attempt < max_attempts - 1:
            delay = _retry_delay(response, attempt)
            time.sleep(delay)
            attempt += 1
            continue

        if response.status_code >= 400:
            message = _extract_error_message(response)
            raise StarlingAPIError(message, status_code=response.status_code)

        try:
            return response.json()
        except ValueError as exc:
            raise StarlingAPIError("Response was not valid JSON") from exc


def _is_duplicate_transfer_error(error: StarlingAPIError) -> bool:
    message = str(error).lower()
    if error.status_code in {409, 422}:
        return True
    if "idempotency_mismatch" in message:
        return True
    return "already" in message and "exist" in message or "duplicate" in message


def _transfer_uid(cycle_id: str, leg: str) -> str:
    raw = f"salary-automation:{cycle_id}:{leg}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _isoformat_utc(value: datetime) -> str:
    utc = _as_utc(value)
    return utc.isoformat().replace("+00:00", "Z")
