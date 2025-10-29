from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx

API_BASE_URL = "https://api.starlingbank.com"


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
class Space:
    uid: str
    name: str
    state: Optional[str]
    balance: Money
    goal_amount: Optional[Money]
    settings: Dict[str, Any] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class AccountReport:
    account_uid: str
    account_name: Optional[str]
    currency: Optional[str]
    default_category: Optional[str]
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
        accounts_data = _request_json(client, "GET", "/api/v2/accounts")
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
                spaces.append(space)
            reports.append(
                AccountReport(
                    account_uid=account_uid,
                    account_name=account.get("name"),
                    currency=account.get("currency"),
                    default_category=account.get("defaultCategory"),
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
            if space.state:
                yield f"    State: {space.state}"
            if space.settings:
                yield "    Settings:"
                for key in sorted(space.settings):
                    yield f"      {key}: {space.settings[key]}"
            else:
                yield "    Settings: none"


def _parse_money(value: Any, *, default_currency: Optional[str] = None) -> Optional[Money]:
    if value is None:
        return None

    if isinstance(value, Money):
        return value

    if isinstance(value, dict):
        currency = value.get("currency") or value.get("currencyCode") or default_currency
        minor_units = value.get("minorUnits") or value.get("minorUnit")
        if currency and minor_units is not None:
            try:
                return Money(currency=currency, minor_units=int(minor_units))
            except (TypeError, ValueError):
                return None

        amount = value.get("amount") or value.get("value")
        if currency and amount is not None:
            try:
                minor_units = int((Decimal(str(amount))) * 100)
                return Money(currency=currency, minor_units=minor_units)
            except (ArithmeticError, ValueError):
                return None

        for candidate in MONEY_KEY_CANDIDATES:
            nested_value = value.get(candidate)
            if nested_value is value:
                continue
            money = _parse_money(
                nested_value,
                default_currency=currency or default_currency,
            )
            if money:
                return money

        currency_key, units_key = _find_currency_minor_pair(value)
        if currency_key and units_key:
            try:
                return Money(
                    currency=str(value[currency_key]),
                    minor_units=int(value[units_key]),
                )
            except (TypeError, ValueError):
                return None

    elif isinstance(value, (int, float)) and default_currency:
        try:
            decimal_value = Decimal(str(value))
        except (ArithmeticError, ValueError):
            return None
        return Money(currency=default_currency, minor_units=int(decimal_value))

    elif isinstance(value, str) and default_currency:
        try:
            decimal_value = Decimal(value)
        except (ArithmeticError, ValueError):
            return None
        return Money(currency=default_currency, minor_units=int(decimal_value))

    return None


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

    balance = None
    for key in BALANCE_KEYS:
        if key in item:
            balance = _parse_money(item.get(key), default_currency=currency_hint)
            if balance:
                break
    if not balance:
        raise StarlingSchemaError(
            f"Missing recognised balance for space {uid}; keys present: {sorted(item.keys())}"
        )

    goal_amount = None
    for key in GOAL_KEYS:
        if key in item:
            money = _parse_money(item.get(key), default_currency=currency_hint)
            if money:
                goal_amount = money
                break

    settings: Dict[str, Any] = {}
    if isinstance(item.get("settings"), dict):
        settings.update(item["settings"])
    for key in ("roundUpMultiplier", "sweepEnabled", "spendProportion"):
        if key in item and key not in settings:
            settings[key] = item[key]

    return Space(
        uid=uid,
        name=name,
        state=item.get("state"),
        balance=balance,
        goal_amount=goal_amount,
        settings=settings,
        raw=item,
    )


def _request_json(client: httpx.Client, method: str, url: str) -> Dict[str, Any]:
    try:
        response = client.request(method, url)
    except httpx.HTTPError as exc:
        raise StarlingAPIError(f"Network error: {exc}") from exc
    if response.status_code >= 400:
        message = _extract_error_message(response)
        raise StarlingAPIError(message, status_code=response.status_code)
    try:
        return response.json()
    except ValueError as exc:
        raise StarlingAPIError("Response was not valid JSON") from exc


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
    if not isinstance(data, dict):
        return []
    for key in ("accounts", "accountList"):
        value = data.get(key)
        if isinstance(value, list):
            return value
    return []


def _extract_spaces(data: Dict[str, Any]) -> Sequence[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    for key in ("spaces", "spaceList", "savingsGoals"):
        value = data.get(key)
        if isinstance(value, list):
            return value
    return []
