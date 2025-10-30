import json
from pathlib import Path

import pytest

from starling_spaces import classification


@pytest.fixture(autouse=True)
def clear_rule_cache():
    classification.reset_rules_cache()
    yield
    classification.reset_rules_cache()


def test_classification_respects_rule_order(tmp_path, monkeypatch):
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        """
rules:
  - type: counterparty_regex
    pattern: "(?i)mortgage"
    category: "Mortgage"
    reason: "counterparty"
  - type: starling_category
    reason: "starling"
""",
        encoding="utf-8",
    )
    monkeypatch.setenv(classification.RULES_ENV_VAR, str(rules_path))

    result = classification.classify_transaction(
        {
            "counterparty": "Bank Mortgage",
            "spending_category": "SAVING",
        }
    )

    assert result.category == "Mortgage"
    assert result.reason == "counterparty"


def test_classification_falls_back_to_starling(tmp_path, monkeypatch):
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text("rules: []\n", encoding="utf-8")
    monkeypatch.setenv(classification.RULES_ENV_VAR, str(rules_path))

    result = classification.classify_transaction(
        {
            "spending_category": "GENERAL",
            "space_name": "Any Space",
        }
    )

    assert result.category == "General"
    assert result.reason == "starling_fallback"


def test_classification_falls_back_to_space_name(tmp_path, monkeypatch):
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text("rules: []\n", encoding="utf-8")
    monkeypatch.setenv(classification.RULES_ENV_VAR, str(rules_path))

    result = classification.classify_transaction(
        {
            "spending_category": None,
            "space_name": "Space One",
        }
    )

    assert result.category == "Space One"
    assert result.reason == "space_name_fallback"
