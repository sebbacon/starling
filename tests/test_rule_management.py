import pytest
from django.test import Client
from django.urls import reverse

from starling_web.spaces.models import ClassificationRule


pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def clear_default_rules():
    ClassificationRule.objects.all().delete()


def test_manage_rules_lists_existing():
    ClassificationRule.objects.create(
        position=0,
        rule_type="counterparty_regex",
        category="Holidays",
        reason="travel",
        pattern="(?i)travel",
    )

    client = Client()
    response = client.get(reverse("spaces:classification-rules"))
    assert response.status_code == 200
    content = response.content.decode()
    assert "Holidays" in content
    assert "Classification Rules" in content


def test_manage_rules_creates_new_rule():
    client = Client(enforce_csrf_checks=False)
    response = client.post(
        reverse("spaces:classification-rules"),
        data={
            "position": 5,
            "rule_type": "space",
            "category": "Mortgage",
            "reason": "space override",
            "space_uid": "space-123",
        },
    )
    assert response.status_code == 302
    rule = ClassificationRule.objects.get()
    assert rule.rule_type == "space"
    assert rule.category == "Mortgage"
    assert rule.space_uid == "space-123"


def test_manage_rules_updates_existing_rule():
    rule = ClassificationRule.objects.create(
        position=1,
        rule_type="space",
        category="Mortgage",
        reason="original",
        space_uid="space-1",
    )

    client = Client(enforce_csrf_checks=False)
    response = client.post(
        reverse("spaces:classification-rules"),
        data={
            "rule_id": rule.id,
            "position": 1,
            "rule_type": "space",
            "category": "Mortgage",
            "reason": "updated",
            "space_uid": "space-1",
        },
    )
    assert response.status_code == 302
    rule.refresh_from_db()
    assert rule.reason == "updated"


def test_manage_rules_deletes_rule():
    rule = ClassificationRule.objects.create(
        position=2,
        rule_type="counterparty_regex",
        category="Food",
        reason="coffee",
        pattern="(?i)coffee",
    )

    client = Client(enforce_csrf_checks=False)
    response = client.post(
        f"{reverse('spaces:classification-rules')}?rule={rule.id}",
        data={
            "rule_id": rule.id,
            "action": "delete",
        },
    )
    assert response.status_code == 302
    assert ClassificationRule.objects.count() == 0
