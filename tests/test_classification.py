
import pytest

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
        }
    )

    assert result.category == "Space One"
    assert result.reason == "space_name_fallback"
