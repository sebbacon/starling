
import pytest
from datetime import date, datetime, timezone

from starling_spaces import classification
from starling_web.spaces.models import ClassificationRule


pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def manage_rules():
    existing = list(
        ClassificationRule.objects.order_by("position", "id").values(
            "position",
            "rule_type",
            "category",
            "reason",
            "pattern",
            "space_uid",
            "json_path",
            "start_date",
            "end_date",
        )
    )
    ClassificationRule.objects.all().delete()
    classification.reset_rules_cache()
    yield
    ClassificationRule.objects.all().delete()
    if existing:
        ClassificationRule.objects.bulk_create(
            ClassificationRule(**record) for record in existing
        )
    classification.reset_rules_cache()


def test_classification_respects_rule_order():
    ClassificationRule.objects.bulk_create(
        [
            ClassificationRule(
                position=0,
                rule_type="counterparty_regex",
                category="Mortgage",
                reason="counterparty",
                pattern="(?i)mortgage",
            ),
            ClassificationRule(
                position=1,
                rule_type="starling_category",
                reason="starling",
            ),
        ]
    )
    classification.reset_rules_cache()

    result = classification.classify_transaction(
        {
            "counterparty": "Bank Mortgage",
            "spending_category": "SAVING",
        }
    )

    assert result.category == "Mortgage"
    assert result.reason == "counterparty"


def test_classification_falls_back_to_starling():
    classification.reset_rules_cache()

    result = classification.classify_transaction(
        {
            "spending_category": "GENERAL",
            "space_name": "Any Space",
            "transaction_time": datetime(2024, 11, 1, tzinfo=timezone.utc),
        }
    )

    assert result.category == "General"
    assert result.reason == "starling_fallback"


def test_classification_falls_back_to_space_name():
    classification.reset_rules_cache()

    result = classification.classify_transaction(
        {
            "spending_category": None,
            "space_name": "Space One",
            "transaction_time": datetime(2024, 11, 1, tzinfo=timezone.utc),
        }
    )

    assert result.category == "Space One"
    assert result.reason == "space_name_fallback"


def test_classification_rule_respects_date_window():
    ClassificationRule.objects.bulk_create(
        [
            ClassificationRule(
                position=0,
                rule_type="counterparty_regex",
                category="Holiday",
                reason="summer",
                pattern="(?i)travel",
                start_date=date(2024, 6, 1),
                end_date=date(2024, 8, 31),
            ),
        ]
    )
    classification.reset_rules_cache()

    inside = classification.classify_transaction(
        {
            "counterparty": "Travel Co",
            "transaction_time": datetime(2024, 7, 15, tzinfo=timezone.utc),
        }
    )
    assert inside.category == "Holiday"
    assert inside.reason == "summer"

    outside = classification.classify_transaction(
        {
            "counterparty": "Travel Co",
            "transaction_time": datetime(2024, 9, 15, tzinfo=timezone.utc),
        }
    )
    assert outside.reason == "fallback"
    assert outside.category == "Uncategorised"
