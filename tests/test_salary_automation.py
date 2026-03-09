import pytest
from datetime import datetime, timezone

import httpx
import respx

from starling_spaces import salary_automation
from starling_spaces.reporting import StarlingAPIError


# ---------------------------------------------------------------------------
# split_into_three_tranches
# ---------------------------------------------------------------------------


def test_split_into_three_tranches_distributes_remainder():
    assert salary_automation.split_into_three_tranches(0) == [0, 0, 0]
    assert salary_automation.split_into_three_tranches(5) == [2, 2, 1]
    assert salary_automation.split_into_three_tranches(10) == [4, 3, 3]


def test_split_into_three_tranches_rejects_negative():
    with pytest.raises(ValueError):
        salary_automation.split_into_three_tranches(-1)


# ---------------------------------------------------------------------------
# due_release_count
# ---------------------------------------------------------------------------


def test_due_release_count_uses_quarter_points():
    salary_time = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)

    assert salary_automation.due_release_count(
        salary_time,
        now=datetime(2026, 1, 7, 23, 59, tzinfo=timezone.utc),
    ) == 0
    assert salary_automation.due_release_count(
        salary_time,
        now=datetime(2026, 1, 8, 0, 0, tzinfo=timezone.utc),
    ) == 1
    assert salary_automation.due_release_count(
        salary_time,
        now=datetime(2026, 1, 15, 9, 0, tzinfo=timezone.utc),
    ) == 2
    assert salary_automation.due_release_count(
        salary_time,
        now=datetime(2026, 1, 23, 9, 0, tzinfo=timezone.utc),
    ) == 3


def test_due_release_count_returns_zero_when_now_equals_salary():
    salary_time = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert salary_automation.due_release_count(salary_time, now=salary_time) == 0


def test_due_release_count_anchors_to_calendar_month_start():
    salary_time = datetime(2026, 2, 26, 10, 0, tzinfo=timezone.utc)

    assert salary_automation.due_release_count(
        salary_time,
        now=datetime(2026, 3, 5, 9, 0, tzinfo=timezone.utc),
    ) == 0
    assert salary_automation.due_release_count(
        salary_time,
        now=datetime(2026, 3, 8, 9, 0, tzinfo=timezone.utc),
    ) == 1


# ---------------------------------------------------------------------------
# _as_utc
# ---------------------------------------------------------------------------


def test_as_utc_handles_naive_datetime():
    naive = datetime(2026, 1, 1, 10, 0)
    result = salary_automation._as_utc(naive)
    assert result.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# _parse_timestamp
# ---------------------------------------------------------------------------


def test_parse_timestamp_handles_naive_iso_string():
    result = salary_automation._parse_timestamp("2026-01-01T10:00:00")
    assert result.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# _normalise_minor_units
# ---------------------------------------------------------------------------


def test_normalise_minor_units_returns_unchanged_without_direction():
    assert salary_automation._normalise_minor_units(1000, None) == 1000


def test_normalise_minor_units_negates_outbound_positive():
    assert salary_automation._normalise_minor_units(1000, "OUT") == -1000


def test_normalise_minor_units_negates_inbound_negative():
    assert salary_automation._normalise_minor_units(-1000, "IN") == 1000


# ---------------------------------------------------------------------------
# _extract_feed_money
# ---------------------------------------------------------------------------


def test_extract_feed_money_returns_none_when_no_money_keys():
    result = salary_automation._extract_feed_money({})
    assert result is None


# ---------------------------------------------------------------------------
# _extract_timestamp
# ---------------------------------------------------------------------------


def test_extract_timestamp_raises_when_no_timestamp_keys():
    with pytest.raises(salary_automation.SalaryAutomationError, match="missing timestamp"):
        salary_automation._extract_timestamp({})


# ---------------------------------------------------------------------------
# _find_latest_salary_payment
# ---------------------------------------------------------------------------


def _oxford_item(**overrides):
    base = {
        "feedItemUid": "uid-1",
        "transactionTime": "2026-01-01T09:00:00Z",
        "amount": {"currency": "GBP", "minorUnits": 550000},
        "direction": "IN",
        "counterPartyName": "University of Oxford Payroll",
    }
    base.update(overrides)
    return base


def test_find_latest_salary_payment_skips_non_matching_counterparty():
    item = _oxford_item(counterPartyName="HMRC")
    result = salary_automation._find_latest_salary_payment([item])
    assert result is None


def test_find_latest_salary_payment_skips_items_without_money():
    item = _oxford_item(amount=None)
    result = salary_automation._find_latest_salary_payment([item])
    assert result is None


def test_find_latest_salary_payment_skips_below_minimum():
    item = _oxford_item(amount={"currency": "GBP", "minorUnits": 100000})
    result = salary_automation._find_latest_salary_payment([item])
    assert result is None


def test_find_latest_salary_payment_skips_above_maximum():
    item = _oxford_item(amount={"currency": "GBP", "minorUnits": 700000})
    result = salary_automation._find_latest_salary_payment([item])
    assert result is None


def test_find_latest_salary_payment_skips_missing_uid():
    item = _oxford_item(feedItemUid=None)
    result = salary_automation._find_latest_salary_payment([item])
    assert result is None


# ---------------------------------------------------------------------------
# _is_duplicate_transfer_error
# ---------------------------------------------------------------------------


def test_is_duplicate_transfer_error_uses_message_heuristic():
    # Hits the message-based check (not the status_code path)
    assert salary_automation._is_duplicate_transfer_error(
        StarlingAPIError("the transfer already exists")
    )
    assert salary_automation._is_duplicate_transfer_error(
        StarlingAPIError("duplicate transfer")
    )
    assert not salary_automation._is_duplicate_transfer_error(
        StarlingAPIError("some other error")
    )


def test_is_duplicate_transfer_error_recognizes_idempotency_mismatch():
    error = StarlingAPIError(
        "{'errors': [{'message': 'IDEMPOTENCY_MISMATCH'}], 'success': False}",
        status_code=400,
    )
    assert salary_automation._is_duplicate_transfer_error(error)


# ---------------------------------------------------------------------------
# _plan_initial_allocations
# ---------------------------------------------------------------------------


def _make_spaces(bills_balance=0, kids_balance=0):
    def snap(uid, name, balance):
        return salary_automation.SpaceSnapshot(
            uid=uid, name=name, balance_minor_units=balance, currency="GBP"
        )

    return {
        "Mortgage (monthly)": snap("s-mort", "Mortgage (monthly)", 0),
        "Groceries (monthly)": snap("s-groc", "Groceries (monthly)", 0),
        "Holidays": snap("s-hols", "Holidays", 0),
        "Bills (monthly)": snap("s-bills", "Bills (monthly)", bills_balance),
        "Kids (monthly)": snap("s-kids", "Kids (monthly)", kids_balance),
        "Salary drawdown": snap("s-draw", "Salary drawdown", 0),
    }


def _top_up_balances_from_spaces(spaces):
    return {
        "Bills (monthly)": spaces["Bills (monthly)"].balance_minor_units,
        "Kids (monthly)": spaces["Kids (monthly)"].balance_minor_units,
    }


def test_plan_initial_allocations_clamps_negative_topup_to_zero():
    # Bills balance already exceeds target; top-up should be clamped to 0
    spaces = _make_spaces(bills_balance=200000, kids_balance=200000)
    planned, _ = salary_automation._plan_initial_allocations(
        spaces=spaces,
        salary_minor_units=550000,
        top_up_balances=_top_up_balances_from_spaces(spaces),
    )
    topup_legs = [t for t in planned if "topup" in t.leg]
    assert all(t.amount_minor_units == 0 for t in topup_legs)


def test_plan_initial_allocations_raises_when_salary_insufficient():
    spaces = _make_spaces()
    with pytest.raises(salary_automation.SalaryAutomationError, match="insufficient"):
        salary_automation._plan_initial_allocations(
            spaces=spaces,
            salary_minor_units=1,
            top_up_balances=_top_up_balances_from_spaces(spaces),
        )


# ---------------------------------------------------------------------------
# _resolve_top_up_cycle_start_balances
# ---------------------------------------------------------------------------


def test_resolve_top_up_cycle_start_balances_rewinds_month_activity(monkeypatch):
    spaces = _make_spaces(bills_balance=90000, kids_balance=15000)
    space_feed_by_category = {
        "s-bills": [
            {
                "amount": {"currency": "GBP", "minorUnits": 10000},
                "direction": "OUT",
            }
        ],
        "s-kids": [
            {
                "amount": {"currency": "GBP", "minorUnits": 10000},
                "direction": "OUT",
            }
        ],
    }

    def fake_fetch(
        client,
        *,
        account_uid,
        category_uid,
        changes_since,
    ):
        return space_feed_by_category[category_uid]

    monkeypatch.setattr(
        "starling_spaces.salary_automation._fetch_feed_items",
        fake_fetch,
    )

    balances = salary_automation._resolve_top_up_cycle_start_balances(
        None,
        account_uid="acc-1",
        spaces=spaces,
        cycle_start=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
    )

    assert balances == {
        "Bills (monthly)": 100000,
        "Kids (monthly)": 25000,
    }


# ---------------------------------------------------------------------------
# _execute_planned_transfer
# ---------------------------------------------------------------------------


def test_execute_planned_transfer_skips_zero_amount():
    transfer = salary_automation.PlannedTransfer(
        leg="test",
        direction="to_space",
        space_name="Test",
        space_uid="space-1",
        amount_minor_units=0,
    )
    result = salary_automation._execute_planned_transfer(
        None,
        account_uid="acc-1",
        currency="GBP",
        cycle_id="cycle-1",
        transfer=transfer,
    )
    assert result == "skipped_zero"


def test_execute_planned_transfer_raises_for_unknown_direction():
    transfer = salary_automation.PlannedTransfer(
        leg="test",
        direction="sideways",
        space_name="Test",
        space_uid="space-1",
        amount_minor_units=1000,
    )
    with pytest.raises(salary_automation.SalaryAutomationError, match="Unknown transfer direction"):
        salary_automation._execute_planned_transfer(
            None,
            account_uid="acc-1",
            currency="GBP",
            cycle_id="cycle-1",
            transfer=transfer,
        )


@respx.mock
def test_execute_planned_transfer_propagates_non_duplicate_errors(respx_mock):
    respx_mock.put(
        url__regex=r"https://api\.starlingbank\.com/api/v2/account/.*/savings-goals/.*/add-money/.*"
    ).respond(400, json={"error": "Bad request"})

    transfer = salary_automation.PlannedTransfer(
        leg="allocation_mortgage",
        direction="to_space",
        space_name="Mortgage (monthly)",
        space_uid="space-mort",
        amount_minor_units=97000,
    )

    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        with pytest.raises(StarlingAPIError):
            salary_automation._execute_planned_transfer(
                client,
                account_uid="acc-1",
                currency="GBP",
                cycle_id="cycle-1",
                transfer=transfer,
            )


# ---------------------------------------------------------------------------
# _request_json_with_payload
# ---------------------------------------------------------------------------


@respx.mock
def test_request_json_with_payload_raises_on_network_error(respx_mock):
    respx_mock.put("https://api.starlingbank.com/test").mock(
        side_effect=httpx.ConnectError("refused")
    )
    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        with pytest.raises(StarlingAPIError, match="Network error"):
            salary_automation._request_json_with_payload(
                client, "PUT", "/test", payload={}
            )


@respx.mock
def test_request_json_with_payload_retries_on_retryable_status(monkeypatch, respx_mock):
    monkeypatch.setattr("time.sleep", lambda _: None)
    attempts = []

    def side_effect(request):
        attempts.append(1)
        if len(attempts) < 2:
            return httpx.Response(500, json={"error": "server error"})
        return httpx.Response(200, json={"success": True})

    respx_mock.put("https://api.starlingbank.com/test").mock(side_effect=side_effect)

    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        result = salary_automation._request_json_with_payload(
            client,
            "PUT",
            "/test",
            payload={},
            max_attempts=2,
            retry_statuses=(500,),
        )

    assert result == {"success": True}
    assert len(attempts) == 2


@respx.mock
def test_request_json_with_payload_raises_on_invalid_json(respx_mock):
    respx_mock.put("https://api.starlingbank.com/test").mock(
        return_value=httpx.Response(200, content=b"not-json")
    )
    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        with pytest.raises(StarlingAPIError, match="not valid JSON"):
            salary_automation._request_json_with_payload(
                client, "PUT", "/test", payload={}
            )


# ---------------------------------------------------------------------------
# run_salary_automation — blank token and no-salary cases
# ---------------------------------------------------------------------------


def test_run_salary_automation_raises_for_blank_token():
    with pytest.raises(salary_automation.SalaryAutomationError, match="STARLING_PAT"):
        salary_automation.run_salary_automation("   ")


@respx.mock
def test_run_salary_automation_returns_no_salary_detected(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={
            "accounts": [
                {
                    "accountUid": "acc-1",
                    "name": "Joint",
                    "currency": "GBP",
                    "defaultCategory": "cat-1",
                }
            ]
        }
    )
    respx_mock.get("https://api.starlingbank.com/api/v2/account/acc-1/spaces").respond(
        json={
            "spaceList": [
                {"spaceUid": "s1", "name": "Mortgage (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s2", "name": "Groceries (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s3", "name": "Holidays", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s4", "name": "Bills (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s5", "name": "Kids (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s6", "name": "Salary drawdown", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
            ]
        }
    )
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-1/category/cat-1"
    ).respond(json={"feedItems": [], "pageable": {"next": None}})

    result = salary_automation.run_salary_automation("TOKEN")

    assert result["status"] == "no_salary_detected"
    assert result["executed"] == 0


# ---------------------------------------------------------------------------
# _resolve_main_account error paths
# ---------------------------------------------------------------------------


@respx.mock
def test_resolve_main_account_raises_when_account_not_found(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={"accounts": [{"accountUid": "acc-1", "name": "Other", "currency": "GBP", "defaultCategory": "cat-1"}]}
    )
    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        with pytest.raises(salary_automation.SalaryAutomationError, match="Account not found"):
            salary_automation._resolve_main_account(client)


@respx.mock
def test_resolve_main_account_raises_for_duplicate_accounts(respx_mock):
    account = {"accountUid": "acc-1", "name": "Joint", "currency": "GBP", "defaultCategory": "cat-1"}
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={"accounts": [account, account]}
    )
    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        with pytest.raises(salary_automation.SalaryAutomationError, match="Multiple accounts"):
            salary_automation._resolve_main_account(client)


@respx.mock
def test_resolve_main_account_raises_for_missing_uid(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={"accounts": [{"name": "Joint", "currency": "GBP", "defaultCategory": "cat-1"}]}
    )
    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        with pytest.raises(salary_automation.SalaryAutomationError, match="missing account UID"):
            salary_automation._resolve_main_account(client)


@respx.mock
def test_resolve_main_account_raises_for_missing_default_category(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={"accounts": [{"accountUid": "acc-1", "name": "Joint", "currency": "GBP"}]}
    )
    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        with pytest.raises(salary_automation.SalaryAutomationError, match="missing defaultCategory"):
            salary_automation._resolve_main_account(client)


@respx.mock
def test_resolve_main_account_raises_for_missing_currency(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={"accounts": [{"accountUid": "acc-1", "name": "Joint", "defaultCategory": "cat-1"}]}
    )
    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        with pytest.raises(salary_automation.SalaryAutomationError, match="missing currency"):
            salary_automation._resolve_main_account(client)


# ---------------------------------------------------------------------------
# _resolve_required_spaces error paths
# ---------------------------------------------------------------------------


@respx.mock
def test_resolve_required_spaces_raises_for_duplicate_space(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/account/acc-1/spaces").respond(
        json={
            "spaceList": [
                {"spaceUid": "s1", "name": "Mortgage (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s2", "name": "Mortgage (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
            ]
        }
    )
    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        with pytest.raises(salary_automation.SalaryAutomationError, match="Duplicate space"):
            salary_automation._resolve_required_spaces(
                client, account_uid="acc-1", account_currency="GBP"
            )


@respx.mock
def test_resolve_required_spaces_raises_for_missing_spaces(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/account/acc-1/spaces").respond(
        json={"spaceList": []}
    )
    with httpx.Client(base_url="https://api.starlingbank.com") as client:
        with pytest.raises(salary_automation.SalaryAutomationError, match="Required spaces missing"):
            salary_automation._resolve_required_spaces(
                client, account_uid="acc-1", account_currency="GBP"
            )


# ---------------------------------------------------------------------------
# dry_run mode
# ---------------------------------------------------------------------------


@respx.mock
def test_run_salary_automation_dry_run_skips_transfers(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={
            "accounts": [
                {
                    "accountUid": "acc-1",
                    "name": "Joint",
                    "currency": "GBP",
                    "defaultCategory": "cat-1",
                }
            ]
        }
    )
    respx_mock.get("https://api.starlingbank.com/api/v2/account/acc-1/spaces").respond(
        json={
            "spaceList": [
                {"spaceUid": "s1", "name": "Mortgage (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s2", "name": "Groceries (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s3", "name": "Holidays", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s4", "name": "Bills (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s5", "name": "Kids (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                {"spaceUid": "s6", "name": "Salary drawdown", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
            ]
        }
    )
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-1/category/cat-1"
    ).respond(
        json={
            "feedItems": [
                {
                    "feedItemUid": "salary-1",
                    "transactionTime": "2026-01-01T09:00:00Z",
                    "amount": {"currency": "GBP", "minorUnits": 550000},
                    "direction": "IN",
                    "counterPartyName": "University of Oxford Payroll",
                }
            ],
            "pageable": {"next": None},
        }
    )
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-1/category/s4"
    ).respond(json={"feedItems": [], "pageable": {"next": None}})
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-1/category/s5"
    ).respond(json={"feedItems": [], "pageable": {"next": None}})

    result = salary_automation.run_salary_automation(
        "TOKEN",
        now=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
        dry_run=True,
    )

    assert result["status"] == "ok"
    assert result["dryRun"] is True
    assert result["executed"] == 0
    assert result["alreadyDone"] == 0
    assert all(a["result"] in {"would_execute", "not_due"} for a in result["actions"])
    # No PUT requests should have been made
    assert not any(r.request.method == "PUT" for r in respx_mock.calls)


# ---------------------------------------------------------------------------
# Drawdown anchoring to month start
# ---------------------------------------------------------------------------


@respx.mock
def test_run_salary_automation_anchors_drawdown_to_month_start_balances(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={
            "accounts": [
                {
                    "accountUid": "acc-1",
                    "name": "Joint",
                    "currency": "GBP",
                    "defaultCategory": "cat-1",
                }
            ]
        }
    )

    spaces_route = respx_mock.get(
        "https://api.starlingbank.com/api/v2/account/acc-1/spaces"
    )
    spaces_route.side_effect = [
        httpx.Response(
            200,
            json={
                "spaceList": [
                    {"spaceUid": "s1", "name": "Mortgage (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                    {"spaceUid": "s2", "name": "Groceries (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                    {"spaceUid": "s3", "name": "Holidays", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                    {"spaceUid": "s4", "name": "Bills (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 100000}},
                    {"spaceUid": "s5", "name": "Kids (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 25000}},
                    {"spaceUid": "s6", "name": "Salary drawdown", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                ]
            },
        ),
        httpx.Response(
            200,
            json={
                "spaceList": [
                    {"spaceUid": "s1", "name": "Mortgage (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                    {"spaceUid": "s2", "name": "Groceries (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                    {"spaceUid": "s3", "name": "Holidays", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                    {"spaceUid": "s4", "name": "Bills (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 90000}},
                    {"spaceUid": "s5", "name": "Kids (monthly)", "totalSaved": {"currency": "GBP", "minorUnits": 15000}},
                    {"spaceUid": "s6", "name": "Salary drawdown", "totalSaved": {"currency": "GBP", "minorUnits": 0}},
                ]
            },
        ),
    ]

    respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-1/category/cat-1"
    ).respond(
        json={
            "feedItems": [
                {
                    "feedItemUid": "salary-1",
                    "transactionTime": "2026-02-26T09:00:00Z",
                    "amount": {"currency": "GBP", "minorUnits": 550000},
                    "direction": "IN",
                    "counterPartyName": "University of Oxford Payroll",
                }
            ],
            "pageable": {"next": None},
        }
    )

    bills_feed_route = respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-1/category/s4"
    )
    bills_feed_route.side_effect = [
        httpx.Response(200, json={"feedItems": [], "pageable": {"next": None}}),
        httpx.Response(
            200,
            json={
                "feedItems": [
                    {
                        "feedItemUid": "bills-spend-1",
                        "amount": {"currency": "GBP", "minorUnits": 10000},
                        "direction": "OUT",
                        "transactionTime": "2026-03-04T09:00:00Z",
                    }
                ],
                "pageable": {"next": None},
            },
        ),
    ]

    kids_feed_route = respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-1/category/s5"
    )
    kids_feed_route.side_effect = [
        httpx.Response(200, json={"feedItems": [], "pageable": {"next": None}}),
        httpx.Response(
            200,
            json={
                "feedItems": [
                    {
                        "feedItemUid": "kids-spend-1",
                        "amount": {"currency": "GBP", "minorUnits": 10000},
                        "direction": "OUT",
                        "transactionTime": "2026-03-05T09:00:00Z",
                    }
                ],
                "pageable": {"next": None},
            },
        ),
    ]

    first = salary_automation.run_salary_automation(
        "TOKEN",
        now=datetime(2026, 3, 1, 9, 0, tzinfo=timezone.utc),
        dry_run=True,
    )
    second = salary_automation.run_salary_automation(
        "TOKEN",
        now=datetime(2026, 3, 10, 9, 0, tzinfo=timezone.utc),
        dry_run=True,
    )

    first_release = next(action for action in first["actions"] if action["leg"] == "release_q2")
    second_release = next(action for action in second["actions"] if action["leg"] == "release_q2")

    assert first_release["amountMinorUnits"] == 79500
    assert first_release["result"] == "not_due"
    assert second_release["amountMinorUnits"] == 79500
    assert second_release["result"] == "would_execute"
    assert second["dueReleaseCount"] == 1


# ---------------------------------------------------------------------------
# Full integration — idempotency
# ---------------------------------------------------------------------------


@respx.mock
def test_run_salary_automation_is_idempotent_with_deterministic_transfer_uids(respx_mock):
    respx_mock.get("https://api.starlingbank.com/api/v2/accounts").respond(
        json={
            "accounts": [
                {
                    "accountUid": "acc-123",
                    "name": "Joint",
                    "currency": "GBP",
                    "defaultCategory": "cat-main",
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
                    "spaceUid": "space-mortgage",
                    "name": "Mortgage (monthly)",
                    "totalSaved": {"currency": "GBP", "minorUnits": 100},
                },
                {
                    "spaceUid": "space-groceries",
                    "name": "Groceries (monthly)",
                    "totalSaved": {"currency": "GBP", "minorUnits": 100},
                },
                {
                    "spaceUid": "space-holidays",
                    "name": "Holidays",
                    "totalSaved": {"currency": "GBP", "minorUnits": 100},
                },
                {
                    "spaceUid": "space-bills",
                    "name": "Bills (monthly)",
                    "totalSaved": {"currency": "GBP", "minorUnits": 100000},
                },
                {
                    "spaceUid": "space-kids",
                    "name": "Kids (monthly)",
                    "totalSaved": {"currency": "GBP", "minorUnits": 25000},
                },
                {
                    "spaceUid": "space-drawdown",
                    "name": "Salary drawdown",
                    "totalSaved": {"currency": "GBP", "minorUnits": 50000},
                },
            ]
        }
    )
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-123/category/cat-main"
    ).respond(
        json={
            "feedItems": [
                {
                    "feedItemUid": "salary-feed-1",
                    "transactionTime": "2026-01-01T09:00:00Z",
                    "amount": {"currency": "GBP", "minorUnits": 550000},
                    "direction": "IN",
                    "source": "BANK_TRANSFER",
                    "counterPartyName": "University of Oxford Payroll",
                }
            ],
            "pageable": {"next": None},
        }
    )
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-123/category/space-bills"
    ).respond(json={"feedItems": [], "pageable": {"next": None}})
    respx_mock.get(
        "https://api.starlingbank.com/api/v2/feed/account/acc-123/category/space-kids"
    ).respond(json={"feedItems": [], "pageable": {"next": None}})

    seen_transfer_uids = set()

    def transfer_callback(request):
        transfer_uid = request.url.path.rsplit("/", 1)[-1]
        if transfer_uid in seen_transfer_uids:
            return httpx.Response(409, json={"error": "Transfer already exists"})
        seen_transfer_uids.add(transfer_uid)
        return httpx.Response(200, json={"success": True})

    respx_mock.put(
        url__regex=r"https://api\.starlingbank\.com/api/v2/account/acc-123/savings-goals/.*/add-money/.*"
    ).mock(side_effect=transfer_callback)
    respx_mock.put(
        url__regex=r"https://api\.starlingbank\.com/api/v2/account/acc-123/savings-goals/.*/withdraw-money/.*"
    ).mock(side_effect=transfer_callback)

    first = salary_automation.run_salary_automation(
        "TOKEN",
        now=datetime(2026, 1, 10, 9, 0, tzinfo=timezone.utc),
    )
    second = salary_automation.run_salary_automation(
        "TOKEN",
        now=datetime(2026, 1, 10, 9, 0, tzinfo=timezone.utc),
    )

    assert len(seen_transfer_uids) == 7
    assert first["status"] == "ok"
    assert second["status"] == "ok"
    assert second["alreadyDone"] >= 1
