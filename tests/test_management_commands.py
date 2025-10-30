import json

import pytest
from django.core.management import CommandError, call_command


def test_ingest_feeds_requires_token(monkeypatch):
    monkeypatch.delenv("STARLING_PAT", raising=False)

    with pytest.raises(CommandError) as excinfo:
        call_command("ingest_feeds", db="ignored.sqlite3")

    assert "STARLING_PAT" in str(excinfo.value)


def test_ingest_feeds_invokes_sync(monkeypatch, tmp_path):
    invocations = []

    def fake_sync(token, **kwargs):
        invocations.append((token, kwargs))

    monkeypatch.setenv("STARLING_PAT", "secret")
    monkeypatch.setattr("starling_spaces.ingestion.sync_space_feeds", fake_sync)

    monkeypatch.chdir(tmp_path)

    call_command("ingest_feeds", db="target.sqlite3")

    assert invocations
    token, kwargs = invocations[0]
    assert token == "secret"
    assert kwargs["db_path"] == tmp_path / "target.sqlite3"


def test_average_spend_outputs_summary(monkeypatch, sample_feed_database, capsys):
    monkeypatch.delenv("STARLING_PAT", raising=False)

    call_command(
        "average_spend",
        db=str(sample_feed_database),
        reference_time="2024-11-15T00:00:00+00:00",
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["spaces"]
    assert payload["spendingCategories"]


def test_report_spaces_emits_json(monkeypatch, capsys):
    monkeypatch.setenv("STARLING_PAT", "TOKEN")
    fake_payload = {"accounts": [{"accountUid": "acc-1"}]}

    monkeypatch.setattr(
        "starling_spaces.reporting.fetch_spaces_configuration",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "starling_spaces.reporting.build_report_payload",
        lambda reports: fake_payload,
    )

    call_command("report_spaces")

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data == fake_payload


def test_ingest_feeds_surfaces_schema_errors(monkeypatch):
    monkeypatch.setenv("STARLING_PAT", "secret")

    def raise_schema_error(*args, **kwargs):
        from starling_spaces.reporting import StarlingSchemaError

        raise StarlingSchemaError("broken")

    monkeypatch.setattr(
        "starling_spaces.ingestion.sync_space_feeds",
        raise_schema_error,
    )

    with pytest.raises(CommandError) as excinfo:
        call_command("ingest_feeds", db="/tmp/example.sqlite3")

    assert "broken" in str(excinfo.value)


def test_average_spend_fetches_account_balances(monkeypatch, capsys):
    summary = {
        "spaces": [{"accountUid": "acc-1", "spaceUid": "space-1", "spaceName": "One"}],
        "spendingCategories": [],
    }

    monkeypatch.setenv("STARLING_PAT", "token")
    monkeypatch.setattr(
        "starling_spaces.ingestion.calculate_average_spend",
        lambda **kwargs: summary,
    )

    captured_balances = {}

    def fake_fetch(token, account_uids, **kwargs):
        captured_balances["token"] = token
        captured_balances["accounts"] = list(account_uids)
        return {"acc-1": {"currency": "GBP", "formatted": "GBP 10.00"}}

    monkeypatch.setattr(
        "starling_spaces.ingestion.fetch_account_balances",
        fake_fetch,
    )

    call_command("average_spend")

    assert captured_balances["token"] == "token"
    assert captured_balances["accounts"] == ["acc-1"]

    payload = json.loads(capsys.readouterr().out)
    assert "accountBalances" in payload


def test_average_spend_handles_balance_errors(monkeypatch, capsys):
    summary = {
        "spaces": [{"accountUid": "acc-1", "spaceUid": "space-1", "spaceName": "One"}],
        "spendingCategories": [],
    }

    monkeypatch.setenv("STARLING_PAT", "token")
    monkeypatch.setattr(
        "starling_spaces.ingestion.calculate_average_spend",
        lambda **kwargs: summary,
    )

    from starling_spaces.reporting import StarlingAPIError

    def raise_api_error(*args, **kwargs):
        raise StarlingAPIError("boom")

    monkeypatch.setattr(
        "starling_spaces.ingestion.fetch_account_balances",
        raise_api_error,
    )

    call_command("average_spend")

    payload = json.loads(capsys.readouterr().out)
    assert "errors" in payload


def test_average_spend_rejects_invalid_reference_time(monkeypatch):
    monkeypatch.delenv("STARLING_PAT", raising=False)

    with pytest.raises(CommandError):
        call_command("average_spend", reference_time="not-a-timestamp")


def test_average_spend_resolves_relative_db_path(monkeypatch, tmp_path):
    captured_paths = {}

    def fake_calculate(**kwargs):
        captured_paths["db_path"] = kwargs["db_path"]
        return {"spaces": [], "spendingCategories": []}

    monkeypatch.setattr("starling_spaces.ingestion.calculate_average_spend", fake_calculate)
    monkeypatch.chdir(tmp_path)

    call_command("average_spend", db="feeds.sqlite3")

    assert captured_paths["db_path"] == tmp_path / "feeds.sqlite3"


def test_average_spend_parses_reference_time_variants():
    from starling_web.spaces.management.commands.average_spend import Command

    command = Command()
    z_time = command._parse_reference_time("2024-01-02T03:04:05Z")
    assert str(z_time.tzinfo) == "UTC"

    naive_time = command._parse_reference_time("2024-01-02T03:04:05")
    assert str(naive_time.tzinfo) == "UTC"


def test_report_spaces_filters_accounts(monkeypatch, capsys):
    monkeypatch.setenv("STARLING_PAT", "token")

    from starling_spaces.reporting import AccountReport, Money, Space

    report = AccountReport(
        account_uid="acc-1",
        account_name="Personal",
        currency="GBP",
        default_category=None,
        balance=Money(currency="GBP", minor_units=1000),
        spaces=[
            Space(
                uid="space-1",
                name="Rainy Day",
                state="ACTIVE",
                balance=Money(currency="GBP", minor_units=1000),
                goal_amount=None,
                settings={},
            )
        ],
    )

    monkeypatch.setattr(
        "starling_spaces.reporting.fetch_spaces_configuration",
        lambda *args, **kwargs: [report],
    )

    call_command("report_spaces", accounts=["acc-2"])

    payload = json.loads(capsys.readouterr().out)
    assert payload == {"accounts": []}


def test_report_spaces_requires_token(monkeypatch):
    monkeypatch.delenv("STARLING_PAT", raising=False)

    with pytest.raises(CommandError):
        call_command("report_spaces")


def test_report_spaces_wraps_errors(monkeypatch):
    monkeypatch.setenv("STARLING_PAT", "token")

    def raise_error(*args, **kwargs):
        from starling_spaces.reporting import StarlingAPIError

        raise StarlingAPIError("boom")

    monkeypatch.setattr(
        "starling_spaces.reporting.fetch_spaces_configuration",
        raise_error,
    )

    with pytest.raises(CommandError) as excinfo:
        call_command("report_spaces")

    assert "boom" in str(excinfo.value)
