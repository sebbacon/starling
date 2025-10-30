import json
import sqlite3

import pytest
import respx
from httpx import Response

from starling_spaces import cli as spaces_cli
from starling_spaces.reporting import (AccountReport, Money, Space,
                                       StarlingAPIError, StarlingSchemaError,
                                       build_report_payload,
                                       fetch_spaces_configuration)


@respx.mock
def test_fetches_spaces_configuration_and_formats_lines(respx_mock):
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
                "minorUnits": 567890,
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
                    "state": "ACTIVE",
                    "totalSaved": {"currency": "GBP", "minorUnits": 120000},
                    "goalAmount": {"currency": "GBP", "minorUnits": 200000},
                    "settings": {
                        "roundUpMultiplier": 2,
                        "sweepEnabled": True,
                    },
                },
                    {
                        "spaceUid": "space-2",
                        "name": "Holiday",
                        "state": "ACTIVE",
                        "totalSaved": {"currency": "GBP", "minorUnits": 3050},
                    "settings": {},
                },
            ]
        }
    )
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/account/acc-123/savings-goals/space-1/recurring-transfer"
    ).mock(
        side_effect=[
            Response(
                429,
                json={"error": "rate limited"},
                headers={"Retry-After": "0.1"},
            ),
            Response(
                200,
                json={
                    "transferUid": "tx-1",
                    "recurrenceRule": {
                        "frequency": "MONTHLY",
                        "interval": 1,
                    },
                    "currencyAndAmount": {"currency": "GBP", "minorUnits": 30000},
                    "nextPaymentDate": "2025-11-02",
                    "topUp": True,
                    "reference": "Budget top-up",
                },
            ),
        ]
    )
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/account/acc-123/savings-goals/space-2/recurring-transfer"
    ).respond(status_code=404, json={"error": "not found"})

    reports = fetch_spaces_configuration("TOKEN")

    assert len(reports) == 1
    report = reports[0]
    assert isinstance(report, AccountReport)
    assert report.account_uid == "acc-123"
    assert report.account_name == "Personal"
    assert len(report.spaces) == 2
    rainy = report.spaces[0]
    assert rainy.balance.formatted == "GBP 1,200.00"
    assert rainy.goal_amount and rainy.goal_amount.formatted == "GBP 2,000.00"
    assert rainy.settings == {"roundUpMultiplier": 2, "sweepEnabled": True}

    assert report.balance
    assert report.balance.formatted == "GBP 5,678.90"

    payload = build_report_payload(reports)
    assert payload["accounts"]
    account_payload = payload["accounts"][0]
    assert account_payload["accountUid"] == "acc-123"
    assert account_payload["balance"]["formatted"] == "GBP 5,678.90"

    rainy_payload = account_payload["spaces"][0]
    assert rainy_payload["name"] == "Rainy Day"
    assert rainy_payload["balance"]["formatted"] == "GBP 1,200.00"
    assert rainy_payload["goalAmount"]["formatted"] == "GBP 2,000.00"
    assert rainy_payload["recurringTransfer"]["amount"]["formatted"] == "GBP 300.00"
    assert rainy_payload["recurringTransfer"]["reference"] == "Budget top-up"

    holiday_payload = account_payload["spaces"][1]
    assert holiday_payload["name"] == "Holiday"
    assert holiday_payload["balance"]["formatted"] == "GBP 30.50"


@respx.mock
def test_fetch_spaces_configuration_raises_on_http_error(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        status_code=401,
        json={"error": "invalid token"},
    )

    with pytest.raises(StarlingAPIError) as excinfo:
        fetch_spaces_configuration("TOKEN")

    assert "401" in str(excinfo.value)


@respx.mock
def test_fetch_spaces_configuration_raises_on_schema_error(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={
            "accounts": [
                {
                    "accountUid": "acc-123",
                    "name": "Personal",
                    "currency": "GBP",
                }
            ]
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
                    # Intentionally missing balance keys
                }
            ]
        }
    )

    with pytest.raises(StarlingSchemaError):
        fetch_spaces_configuration("TOKEN")


def test_build_report_payload_handles_empty():
    payload = build_report_payload([])
    assert payload == {"accounts": []}


def test_cli_outputs_report(monkeypatch, capsys):
    space = Space(
        uid="space-1",
        name="Rainy Day",
        state="ACTIVE",
        balance=Money(currency="GBP", minor_units=120000),
        goal_amount=None,
        settings={"roundUpMultiplier": 2},
    )
    report = AccountReport(
        account_uid="acc-1",
        account_name="Personal",
        currency="GBP",
        default_category=None,
        balance=Money(currency="GBP", minor_units=250000),
        spaces=[space],
    )

    monkeypatch.setattr(
        spaces_cli,
        "fetch_spaces_configuration",
        lambda *_args, **_kwargs: [report],
    )
    monkeypatch.setattr(spaces_cli, "load_dotenv", lambda: None)
    monkeypatch.setenv("STARLING_PAT", "TOKEN")

    exit_code = spaces_cli.main([])
    assert exit_code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["accounts"][0]["accountUid"] == "acc-1"
    assert data["accounts"][0]["balance"]["formatted"] == "GBP 2,500.00"
    assert data["accounts"][0]["spaces"][0]["balance"]["formatted"] == "GBP 1,200.00"


def test_cli_requires_token(monkeypatch):
    monkeypatch.setattr(spaces_cli, "fetch_spaces_configuration", lambda *_: [])
    monkeypatch.setattr(spaces_cli, "load_dotenv", lambda: None)
    monkeypatch.delenv("STARLING_PAT", raising=False)

    exit_code = spaces_cli.main([])
    assert exit_code == 1


def test_cli_surfaces_schema_errors(monkeypatch, capsys):
    def _raise_schema_error(*_args, **_kwargs):
        raise StarlingSchemaError("broken schema")

    monkeypatch.setattr(
        spaces_cli,
        "fetch_spaces_configuration",
        _raise_schema_error,
    )
    monkeypatch.setattr(spaces_cli, "load_dotenv", lambda: None)
    monkeypatch.setenv("STARLING_PAT", "TOKEN")

    exit_code = spaces_cli.main([])
    assert exit_code == 3
    captured = capsys.readouterr()
    assert "Unexpected response schema" in captured.err


def test_cli_average_spend_outputs_categories(monkeypatch, tmp_path, capsys):
    db_path = tmp_path / "avg.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE categories (account_uid TEXT, category_type TEXT, category_uid TEXT, space_uid TEXT, name TEXT)"
    )
    conn.execute(
        "CREATE TABLE feed_items (feed_item_uid TEXT, account_uid TEXT, category_uid TEXT, space_uid TEXT, direction TEXT, amount_minor_units INTEGER, currency TEXT, transaction_time TEXT, source TEXT, counterparty TEXT, spending_category TEXT, raw_json TEXT)"
    )
    conn.execute(
        "CREATE TABLE sync_state (account_uid TEXT, category_uid TEXT, last_transaction_time TEXT)"
    )
    conn.execute(
        "INSERT INTO categories VALUES ('acc-1', 'space', 'space-1', 'space-1', 'Space One')"
    )
    conn.execute(
        "INSERT INTO categories VALUES ('acc-1', 'spending', 'SHOPPING', NULL, 'Shopping')"
    )
    conn.execute(
        """
        INSERT INTO feed_items VALUES (
            'item-1',
            'acc-1',
            'space-1',
            'space-1',
            'OUT',
            -2500,
            'GBP',
            '2024-11-10T12:00:00+00:00',
            NULL,
            'Merchant',
            'SHOPPING',
            '{}'
        )
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(spaces_cli, "load_dotenv", lambda: None)
    monkeypatch.setattr(spaces_cli, "_get_token", lambda: None)

    exit_code = spaces_cli.main(
        [
            "average-spend",
            "--db",
            str(db_path),
            "--reference-time",
            "2024-11-15T00:00:00+00:00",
        ]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert any(item["spaceUid"] == "space-1" for item in data["spaces"])
    assert any(item["category"] == "SHOPPING" for item in data["spendingCategories"])
    assert "accountBalances" not in data
