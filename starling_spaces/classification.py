from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml


RULES_ENV_VAR = "STARLING_CLASSIFICATION_RULES"
DEFAULT_RULES_PATH = Path(__file__).resolve().parent.parent / "config" / "classification_rules.yaml"


@dataclass(frozen=True)
class Classification:
    category: str
    reason: str


def classify_transaction(transaction: Dict[str, Any]) -> Classification:
    for rule in _load_rules():
        result = _apply_rule(rule, transaction)
        if result:
            return result
    return Classification(category="Uncategorised", reason="fallback")


def _apply_rule(rule: Dict[str, Any], tx: Dict[str, Any]) -> Optional[Classification]:
    rule_type = rule.get("type")
    reason = rule.get("reason", rule_type or "rule")

    if rule_type == "space":
        space_uid = rule.get("space_uid")
        if space_uid and tx.get("space_uid") == space_uid:
            return Classification(category=rule["category"], reason=reason)
    elif rule_type == "space_name_regex":
        pattern = rule.get("pattern")
        name = tx.get("space_name") or ""
        if pattern and name and re.search(pattern, name):
            return Classification(category=rule["category"], reason=reason)
    elif rule_type == "counterparty_regex":
        pattern = rule.get("pattern")
        counterparty = tx.get("counterparty") or ""
        if pattern and counterparty and re.search(pattern, counterparty):
            return Classification(category=rule["category"], reason=reason)
    elif rule_type == "source_regex":
        pattern = rule.get("pattern")
        source = tx.get("source") or ""
        if pattern and source and re.search(pattern, source):
            return Classification(category=rule["category"], reason=reason)
    elif rule_type == "starling_category":
        category = tx.get("spending_category")
        if category:
            return Classification(
                category=rule.get("category", _format_category(category)),
                reason=reason,
            )
    elif rule_type == "space_name":
        space_name = tx.get("space_name")
        if space_name:
            return Classification(category=space_name, reason=reason)
    elif rule_type == "raw_path":
        json_path = rule.get("path")
        category = rule.get("category")
        if json_path and category:
            if _match_json_path(tx.get("raw"), json_path):
                return Classification(category=category, reason=reason)

    return None


def _match_json_path(raw: Any, path: str) -> bool:
    if raw is None:
        return False
    current = raw
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return False
    if isinstance(current, (str, int, float)):
        return bool(current)
    return current is not None


@lru_cache(maxsize=1)
def _load_rules() -> List[Dict[str, Any]]:
    configured = _load_configured_rules()
    # Fallbacks always applied in order after configured rules
    fallbacks: List[Dict[str, Any]] = [
        {"type": "starling_category", "reason": "starling_fallback"},
        {"type": "space_name", "reason": "space_name_fallback"},
    ]
    return configured + fallbacks


@lru_cache(maxsize=1)
def _load_configured_rules() -> List[Dict[str, Any]]:
    path = Path(os.getenv(RULES_ENV_VAR) or DEFAULT_RULES_PATH)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    rules = data.get("rules")
    if not rules:
        return []
    validated: List[Dict[str, Any]] = []
    for entry in rules:
        if not isinstance(entry, dict):
            continue
        rule_type = entry.get("type")
        if rule_type in {"starling_category", "space_name"}:
            # allow but optional category override
            validated.append(entry)
        elif rule_type in {"space", "space_name_regex", "counterparty_regex", "source_regex", "raw_path"}:
            validated.append(entry)
    return validated


def _format_category(value: str) -> str:
    cleaned = value.replace("_", " ")
    return cleaned.title()


def classify_for_storage(record, space_name: Optional[str]) -> Classification:
    transaction = {
        "space_uid": getattr(record, "space_uid", None),
        "space_name": space_name,
        "spending_category": getattr(record, "spending_category", None),
        "counterparty": getattr(record, "counterparty", None),
        "source": getattr(record, "source", None),
        "amount_minor_units": getattr(record, "amount_minor_units", None),
        "direction": getattr(record, "direction", None),
        "raw": getattr(record, "raw", None),
    }
    return classify_transaction(transaction)


def reset_rules_cache() -> None:
    _load_rules.cache_clear()
    _load_configured_rules.cache_clear()
