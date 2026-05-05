from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("spaces", "0007_classificationrule_amount_bounds"),
    ]

    operations = [
        migrations.CreateModel(
            name="HolidayMerchantOverride",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("merchant_key", models.CharField(max_length=255, unique=True)),
                ("label", models.CharField(blank=True, max_length=255, null=True)),
                ("override_type", models.CharField(max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ("merchant_key",),
            },
        ),
        migrations.CreateModel(
            name="HolidaySuggestionDecision",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("feed_item_uid", models.CharField(max_length=64, unique=True)),
                ("decision", models.CharField(max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ("feed_item_uid",),
            },
        ),
    ]
