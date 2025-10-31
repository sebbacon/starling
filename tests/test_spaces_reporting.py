
import pytest
import respx
from httpx import Response

from starling_spaces.reporting import (AccountReport, StarlingAPIError, StarlingSchemaError,
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

