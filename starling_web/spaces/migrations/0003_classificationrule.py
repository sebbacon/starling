from django.db import migrations, models

DEFAULT_RULES = [
    {
        "type": "space_name_regex",
        "category": "Mortgage",
        "reason": "space_name_override",
        "pattern": "(?i)mortgage",
    },
    {
        "type": "counterparty_regex",
        "category": "Mortgage",
        "reason": "counterparty_override",
        "pattern": "(?i)mortgage",
    },
    {
        "type": "counterparty_regex",
        "category": "Savings",
        "reason": "counterparty_override",
        "pattern": "Hargreaves Lansdown",
    },
    {
        "type": "counterparty_regex",
        "category": "Holidays",
        "reason": "counterparty_override",
        "pattern": "Barclaycard",
    },
]


def load_initial_rules(apps, schema_editor):
    rule_model = apps.get_model("spaces", "ClassificationRule")
    for index, entry in enumerate(DEFAULT_RULES):
        rule_model.objects.update_or_create(
            position=index,
            defaults={
                "rule_type": entry.get("type"),
                "category": entry.get("category") or None,
                "reason": entry.get("reason") or None,
                "pattern": entry.get("pattern") or None,
                "space_uid": entry.get("space_uid") or None,
                "json_path": entry.get("path") or None,
            },
        )


class Migration(migrations.Migration):

    dependencies = [
        ("spaces", "0002_alter_feeditem_space_uid"),
    ]

    operations = [
        migrations.CreateModel(
            name="ClassificationRule",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("position", models.PositiveIntegerField(unique=True)),
                ("rule_type", models.CharField(max_length=32)),
                ("category", models.CharField(blank=True, max_length=255, null=True)),
                ("reason", models.CharField(blank=True, max_length=128, null=True)),
                ("pattern", models.CharField(blank=True, max_length=255, null=True)),
                ("space_uid", models.CharField(blank=True, max_length=64, null=True)),
                ("json_path", models.CharField(blank=True, max_length=255, null=True)),
            ],
            options={
                "ordering": ("position", "id"),
            },
        ),
        migrations.RunPython(load_initial_rules, migrations.RunPython.noop),
    ]
