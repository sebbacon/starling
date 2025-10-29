import pytest
import respx
from httpx import Response

from starling_spaces import cli as spaces_cli
from starling_spaces.reporting import (AccountReport, Money, Space,
                                       StarlingAPIError, StarlingSchemaError,
                                       fetch_spaces_configuration,
                                       iter_report_lines)


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

    lines = list(iter_report_lines(reports))
    assert "Account Personal (acc-123) — GBP" in lines[0]
    assert any("Rainy Day (space-1)" in line for line in lines)
    assert any("Balance: GBP 1,200.00" in line for line in lines)
    assert any("Target: GBP 2,000.00" in line for line in lines)
    assert any(
        "Recurring transfer: GBP 300.00 (monthly), next on 2025-11-02 [top-up]"
        in line
        for line in lines
    )
    assert any("Reference: Budget top-up" in line for line in lines)
    assert any("roundUpMultiplier: 2" in line for line in lines)
    assert any("sweepEnabled: True" in line for line in lines)
    assert any("Holiday (space-2)" in line for line in lines)
    assert any("Balance: GBP 30.50" in line for line in lines)


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


def test_iter_report_lines_handles_empty(monkeypatch):
    lines = list(iter_report_lines([]))
    assert lines == ["No accounts with Spaces were found."]


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
    assert "Account Personal (acc-1)" in captured.out
    assert "Rainy Day" in captured.out


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
