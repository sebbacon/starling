
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
            "min_amount_minor_units",
            "max_amount_minor_units",
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


def test_classification_rule_can_match_counterparty_and_minimum_income_amount():
    ClassificationRule.objects.create(
        position=0,
        rule_type="counterparty_regex",
        category="Salary",
        reason="salary",
        pattern="University Of Oxfo",
        min_amount_minor_units=500000,
    )
    classification.reset_rules_cache()

    matched = classification.classify_transaction(
        {
            "counterparty": "University Of Oxfo",
            "amount_minor_units": 591714,
            "transaction_time": datetime(2025, 3, 28, tzinfo=timezone.utc),
        }
    )
    assert matched.category == "Salary"
    assert matched.reason == "salary"

    below_threshold = classification.classify_transaction(
        {
            "counterparty": "University Of Oxfo",
            "amount_minor_units": 49999,
            "transaction_time": datetime(2025, 3, 28, tzinfo=timezone.utc),
        }
    )
    assert below_threshold.category == "Uncategorised"
    assert below_threshold.reason == "fallback"


def test_classification_rule_can_match_counterparty_and_maximum_income_amount():
    ClassificationRule.objects.create(
        position=0,
        rule_type="counterparty_regex",
        category="Expenses",
        reason="small_income",
        pattern="(?i)University Of Oxfo",
        min_amount_minor_units=1,
        max_amount_minor_units=49999,
    )
    classification.reset_rules_cache()

    matched = classification.classify_transaction(
        {
            "counterparty": "University Of Oxfo",
            "amount_minor_units": 10995,
            "transaction_time": datetime(2025, 3, 28, tzinfo=timezone.utc),
        }
    )
    assert matched.category == "Expenses"
    assert matched.reason == "small_income"

    above_threshold = classification.classify_transaction(
        {
            "counterparty": "University Of Oxfo",
            "amount_minor_units": 50000,
            "transaction_time": datetime(2025, 3, 28, tzinfo=timezone.utc),
        }
    )
    assert above_threshold.category == "Uncategorised"
    assert above_threshold.reason == "fallback"


def test_classification_rule_can_match_amount_range_without_other_fields():
    ClassificationRule.objects.create(
        position=0,
        rule_type="amount_range",
        category="Expenses",
        reason="small_income",
        min_amount_minor_units=1,
        max_amount_minor_units=49999,
    )
    classification.reset_rules_cache()

    matched = classification.classify_transaction(
        {
            "amount_minor_units": 10995,
            "transaction_time": datetime(2025, 3, 28, tzinfo=timezone.utc),
        }
    )
    assert matched.category == "Expenses"
    assert matched.reason == "small_income"

    above_threshold = classification.classify_transaction(
        {
            "amount_minor_units": 50000,
            "transaction_time": datetime(2025, 3, 28, tzinfo=timezone.utc),
        }
    )
    assert above_threshold.category == "Uncategorised"
    assert above_threshold.reason == "fallback"


def test_classification_can_remap_matching_classified_category():
    ClassificationRule.objects.bulk_create(
        [
            ClassificationRule(
                position=0,
                rule_type="counterparty_regex",
                category="Entertainment",
                reason="merchant_match",
                pattern="(?i)netflix",
            ),
            ClassificationRule(
                position=1,
                rule_type="classified_category_regex",
                category="Lifestyle & Entertainment",
                reason="combined_category",
                pattern="(?i)^(Lifestyle|Entertainment)$",
            ),
        ]
    )
    classification.reset_rules_cache()

    result = classification.classify_transaction(
        {
            "counterparty": "Netflix",
            "transaction_time": datetime(2025, 3, 28, tzinfo=timezone.utc),
        }
    )

    assert result.category == "Lifestyle & Entertainment"
    assert result.reason == "combined_category"
