from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx

API_BASE_URL = "https://api.starlingbank.com"
RETRYABLE_STATUS_CODES: Tuple[int, ...] = (429, 500, 502, 503, 504)
MIN_RETRY_DELAY_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 5.0


class StarlingAPIError(RuntimeError):
    """Raised when the Starling API responds with an error."""

    def __init__(self, message: str, *, status_code: Optional[int] = None) -> None:
        details = message
        if status_code is not None:
            details = f"[{status_code}] {message}"
        super().__init__(details)
        self.status_code = status_code


class StarlingSchemaError(RuntimeError):
    """Raised when the Starling API returns data with an unexpected shape."""


@dataclass(frozen=True)
class Money:
    currency: str
    minor_units: int

    @property
    def formatted(self) -> str:
        amount = Decimal(self.minor_units) / Decimal("100")
        return f"{self.currency} {format(amount, ',.2f')}"


@dataclass(frozen=True)
class RecurringTransfer:
    transfer_uid: str
    amount: Money
    frequency: str
    interval: Optional[int]
    next_payment_date: Optional[str]
    top_up: Optional[bool]
    reference: Optional[str]
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class Space:
    uid: str
    name: str
    state: Optional[str]
    balance: Money
    goal_amount: Optional[Money]
    settings: Dict[str, Any] = field(default_factory=dict)
    recurring_transfer: Optional[RecurringTransfer] = None
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class AccountReport:
    account_uid: str
    account_name: Optional[str]
    currency: Optional[str]
    default_category: Optional[str]
    balance: Optional[Money] = None
    spaces: Sequence[Space] = field(default_factory=list)


BALANCE_KEYS: Tuple[str, ...] = (
    "balance",
    "totalSaved",
    "savingsBalance",
    "currentBalance",
    "availableBalance",
    "balanceAmount",
    "spaceBalance",
)

GOAL_KEYS: Tuple[str, ...] = (
    "goalAmount",
    "target",
    "targetAmount",
    "targetAmountInMinorUnits",
    "targetBalance",
)

ACCOUNT_BALANCE_KEYS: Tuple[str, ...] = (
    "effectiveBalance",
    "currentBalance",
    "availableToSpend",
    "clearedBalance",
    "accountBalance",
    "balance",
)

MONEY_KEY_CANDIDATES: Tuple[str, ...] = (
    "balance",
    "balanceAmount",
    "balanceValue",
    "totalBalance",
    "totalSaved",
    "availableBalance",
    "savingsBalance",
    "currentBalance",
    "spaceBalance",
    "amount",
    "value",
    "minorUnits",
    "minorUnit",
    "currencyAndAmount",
    "potBalance",
)


def fetch_spaces_configuration(
    token: str,
    *,
    base_url: str = API_BASE_URL,
    timeout: float = 10.0,
) -> List[AccountReport]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    with httpx.Client(base_url=base_url, headers=headers, timeout=timeout) as client:
        accounts_data = _request_json(
            client,
            "GET",
            "/api/v2/accounts",
            max_attempts=3,
            retry_statuses=RETRYABLE_STATUS_CODES,
        )
        account_items = _extract_accounts(accounts_data)
        reports: List[AccountReport] = []
        for account in account_items:
            account_uid = account.get("accountUid") or account.get("id")
            if not account_uid:
                continue

            spaces_data = _request_json(
                client,
                "GET",
                f"/api/v2/account/{account_uid}/spaces",
                max_attempts=3,
                retry_statuses=RETRYABLE_STATUS_CODES,
            )
            space_items = _extract_spaces(spaces_data)
            spaces: List[Space] = []
            for space_item in space_items:
                try:
                    space = _parse_space(
                        space_item,
                        account_currency=account.get("currency"),
                    )
                except StarlingSchemaError as exc:
                    raise StarlingSchemaError(
                        f"{exc} (accountUid={account_uid})"
                    ) from exc
                recurring = _fetch_recurring_transfer(
                    client,
                    account_uid=account_uid,
                    space_uid=space.uid,
                    currency_hint=space.balance.currency,
                )
                if recurring:
                    space = replace(space, recurring_transfer=recurring)
                spaces.append(space)
            balance = _fetch_account_balance(
                client,
                account_uid=account_uid,
                currency_hint=account.get("currency"),
            )
            reports.append(
                AccountReport(
                    account_uid=account_uid,
                    account_name=account.get("name"),
                    currency=account.get("currency"),
                    default_category=account.get("defaultCategory"),
                    balance=balance,
                    spaces=spaces,
                )
            )
    return reports


def iter_report_lines(reports: Sequence[AccountReport]) -> Iterable[str]:
    if not reports:
        yield "No accounts with Spaces were found."
        return

    for report in reports:
        account_name = report.account_name or "Unnamed account"
        currency = report.currency or "Unknown currency"
        yield f"Account {account_name} ({report.account_uid}) — {currency}"
        if not report.spaces:
            yield "  No Spaces found."
            continue
        for space in report.spaces:
            yield f"  Space {space.name} ({space.uid})"
            yield f"    Balance: {space.balance.formatted}"
            if space.goal_amount:
                yield f"    Target: {space.goal_amount.formatted}"
            if space.recurring_transfer:
                yield _format_recurring_transfer(space.recurring_transfer)
                if space.recurring_transfer.reference:
                    yield f"      Reference: {space.recurring_transfer.reference}"
            if space.state:
                yield f"    State: {space.state}"
            if space.settings:
                yield "    Settings:"
                for key in sorted(space.settings):
                    yield f"      {key}: {space.settings[key]}"
            else:
                yield "    Settings: none"


def build_report_payload(reports: Sequence[AccountReport]) -> Dict[str, Any]:
    return {
        "accounts": [
            {
                "accountUid": report.account_uid,
                "accountName": report.account_name,
                "currency": report.currency,
                "defaultCategory": report.default_category,
                "balance": _money_to_dict(report.balance),
                "spaces": [_space_to_dict(space) for space in report.spaces],
            }
            for report in reports
        ]
    }


def _space_to_dict(space: Space) -> Dict[str, Any]:
    return {
        "spaceUid": space.uid,
        "name": space.name,
        "state": space.state,
        "balance": _money_to_dict(space.balance),
        "goalAmount": _money_to_dict(space.goal_amount),
        "settings": dict(space.settings),
        "recurringTransfer": _recurring_transfer_to_dict(space.recurring_transfer),
    }


def _recurring_transfer_to_dict(rt: Optional[RecurringTransfer]) -> Optional[Dict[str, Any]]:
    if rt is None:
        return None
    return {
        "transferUid": rt.transfer_uid,
        "amount": _money_to_dict(rt.amount),
        "frequency": rt.frequency,
        "interval": rt.interval,
        "nextPaymentDate": rt.next_payment_date,
        "topUp": rt.top_up,
        "reference": rt.reference,
    }


def _money_to_dict(money: Optional[Money]) -> Optional[Dict[str, Any]]:
    if money is None:
        return None
    return {
        "currency": money.currency,
        "minorUnits": money.minor_units,
        "formatted": money.formatted,
    }


def _parse_money(value: Any, *, default_currency: Optional[str] = None) -> Optional[Money]:
    if value is None:
        return None
    if isinstance(value, Money):
        return value
    if isinstance(value, dict):
        return _money_from_dict(value, default_currency)
    if default_currency and isinstance(value, (int, float, str)):
        return _money_from_scalar(value, default_currency)
    return None


def _money_from_dict(data: Dict[str, Any], default_currency: Optional[str]) -> Optional[Money]:
    currency = data.get("currency") or data.get("currencyCode") or default_currency
    minor_units = data.get("minorUnits") or data.get("minorUnit")
    if currency and minor_units is not None:
        try:
            return Money(currency=currency, minor_units=int(minor_units))
        except (TypeError, ValueError):
            return None

    amount = data.get("amount") or data.get("value")
    if currency and amount is not None:
        try:
            cents = int(Decimal(str(amount)) * 100)
            return Money(currency=currency, minor_units=cents)
        except (InvalidOperation, TypeError, ValueError):
            return None

    for candidate in MONEY_KEY_CANDIDATES:
        if candidate not in data:
            continue
        nested_value = data[candidate]
        if nested_value is data:
            continue
        money = _parse_money(nested_value, default_currency=currency or default_currency)
        if money:
            return money

    currency_key, units_key = _find_currency_minor_pair(data)
    if currency_key and units_key:
        try:
            return Money(
                currency=str(data[currency_key]),
                minor_units=int(data[units_key]),
            )
        except (TypeError, ValueError):
            return None
    return None


def _money_from_scalar(value: Any, currency: str) -> Optional[Money]:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    try:
        minor_units = int(decimal_value)
    except (TypeError, ValueError):
        return None
    return Money(currency=currency, minor_units=minor_units)


def _find_currency_minor_pair(data: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    currency_key = None
    units_key = None
    for key in data:
        lower = key.lower()
        if "currency" in lower and currency_key is None:
            currency_key = key
        if "minorunit" in lower or lower.endswith("minorunits"):
            units_key = key
    return currency_key, units_key


def _parse_space(item: Dict[str, Any], *, account_currency: Optional[str]) -> Space:
    if not isinstance(item, dict):
        raise StarlingSchemaError("Space entry is not an object")

    uid = (
        item.get("spaceUid")
        or item.get("uid")
        or item.get("id")
        or item.get("spaceId")
        or item.get("savingsGoalUid")
    )
    if not uid:
        raise StarlingSchemaError("Missing space UID")

    name = item.get("name") or item.get("spaceName")
    if not name:
        raise StarlingSchemaError(f"Missing space name for uid {uid}")

    currency_hint = (
        item.get("currency")
        or item.get("spaceCurrency")
        or item.get("balanceCurrency")
        or account_currency
    )

    balance = _first_money(item, BALANCE_KEYS, currency_hint)
    if not balance:
        raise StarlingSchemaError(
            f"Missing recognised balance for space {uid}; keys present: {sorted(item.keys())}"
        )

    goal_amount = _first_money(item, GOAL_KEYS, currency_hint)
    settings = _collect_settings(item)

    return Space(
        uid=uid,
        name=name,
        state=item.get("state"),
        balance=balance,
        goal_amount=goal_amount,
        settings=settings,
        raw=item,
    )


def _first_money(
    container: Dict[str, Any],
    keys: Tuple[str, ...],
    currency_hint: Optional[str],
) -> Optional[Money]:
    for key in keys:
        if key not in container:
            continue
        money = _parse_money(container.get(key), default_currency=currency_hint)
        if money:
            return money
    return None


def _collect_settings(item: Dict[str, Any]) -> Dict[str, Any]:
    settings: Dict[str, Any] = {}
    base_settings = item.get("settings")
    if isinstance(base_settings, dict):
        settings.update(base_settings)
    for key in ("roundUpMultiplier", "sweepEnabled", "spendProportion"):
        if key in item and key not in settings:
            settings[key] = item[key]
    return settings


def _format_recurring_transfer(rt: RecurringTransfer) -> str:
    schedule = _describe_schedule(rt.frequency, rt.interval)
    line = f"    Recurring transfer: {rt.amount.formatted} ({schedule})"
    if rt.next_payment_date:
        line += f", next on {rt.next_payment_date}"
    if rt.top_up:
        line += " [top-up]"
    return line


def _describe_schedule(frequency: str, interval: Optional[int]) -> str:
    freq = frequency.replace("_", " ").lower()
    if not interval or interval == 1:
        return freq
    return f"every {interval} {freq}"


def _fetch_account_balance(
    client: httpx.Client,
    *,
    account_uid: str,
    currency_hint: Optional[str],
) -> Money:
    data = _request_json(
        client,
        "GET",
        f"/api/v2/accounts/{account_uid}/balance",
        max_attempts=3,
        retry_statuses=RETRYABLE_STATUS_CODES,
    )
    balance = _parse_account_balance(data, currency_hint=currency_hint)
    if not balance:
        raise StarlingSchemaError(
            f"Missing recognised account balance for account {account_uid}"
        )
    return balance


def _parse_account_balance(
    data: Any,
    *,
    currency_hint: Optional[str],
) -> Optional[Money]:
    money = _parse_money(data, default_currency=currency_hint)
    if money:
        return money
    if isinstance(data, dict):
        for key in ACCOUNT_BALANCE_KEYS:
            if key not in data:
                continue
            nested_money = _parse_money(data[key], default_currency=currency_hint)
            if nested_money:
                return nested_money
        for value in data.values():
            nested_money = _parse_account_balance(value, currency_hint=currency_hint)
            if nested_money:
                return nested_money
    elif isinstance(data, list):
        for item in data:
            nested_money = _parse_account_balance(item, currency_hint=currency_hint)
            if nested_money:
                return nested_money
    return None


def _fetch_recurring_transfer(
    client: httpx.Client,
    *,
    account_uid: str,
    space_uid: str,
    currency_hint: Optional[str],
) -> Optional[RecurringTransfer]:
    url = f"/api/v2/account/{account_uid}/savings-goals/{space_uid}/recurring-transfer"
    try:
        data = _request_json(
            client,
            "GET",
            url,
            max_attempts=3,
            retry_statuses=RETRYABLE_STATUS_CODES,
        )
    except StarlingAPIError as exc:
        if exc.status_code == 404:
            return None
        raise
    return _parse_recurring_transfer(data, currency_hint=currency_hint)


def _parse_recurring_transfer(
    data: Dict[str, Any],
    *,
    currency_hint: Optional[str],
) -> Optional[RecurringTransfer]:
    if not isinstance(data, dict):
        raise StarlingSchemaError("Recurring transfer payload is not an object")

    transfer_uid = data.get("transferUid")
    if not transfer_uid:
        raise StarlingSchemaError("Recurring transfer missing transferUid")

    amount = _parse_money(data.get("currencyAndAmount"), default_currency=currency_hint)
    if not amount:
        raise StarlingSchemaError("Recurring transfer missing currencyAndAmount")

    rule = data.get("recurrenceRule") or {}
    frequency = rule.get("frequency")
    if not frequency:
        raise StarlingSchemaError("Recurring transfer missing frequency")
    interval = rule.get("interval")

    return RecurringTransfer(
        transfer_uid=transfer_uid,
        amount=amount,
        frequency=str(frequency),
        interval=interval,
        next_payment_date=data.get("nextPaymentDate"),
        top_up=data.get("topUp"),
        reference=data.get("reference"),
        raw=data,
    )


def _request_json(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    max_attempts: int = 1,
    retry_statuses: Tuple[int, ...] = (),
) -> Dict[str, Any]:
    attempt = 0
    while True:
        try:
            response = client.request(method, url, params=params)
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


def _retry_delay(response: httpx.Response, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        parsed = _parse_retry_after(retry_after)
        if parsed is not None:
            return max(parsed, MIN_RETRY_DELAY_SECONDS)
    return min((2 ** attempt), MAX_BACKOFF_SECONDS)


def _parse_retry_after(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        retry_time = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if retry_time is None:
        return None
    if retry_time.tzinfo is None:
        retry_time = retry_time.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    delta = (retry_time - now).total_seconds()
    return delta if delta > 0 else None


def _extract_error_message(response: httpx.Response) -> str:
    try:
        data = response.json()
    except ValueError:
        return response.text or "Unknown error"
    if isinstance(data, dict):
        for key in ("error", "message", "errorMessage"):
            if key in data:
                return str(data[key])
    return str(data)


def _extract_accounts(data: Dict[str, Any]) -> Sequence[Dict[str, Any]]:
    return _extract_list(data, ("accounts", "accountList"))


def _extract_spaces(data: Dict[str, Any]) -> Sequence[Dict[str, Any]]:
    return _extract_list(data, ("spaces", "spaceList", "savingsGoals"))


def _extract_list(data: Dict[str, Any], keys: Tuple[str, ...]) -> Sequence[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            return value
    return []
