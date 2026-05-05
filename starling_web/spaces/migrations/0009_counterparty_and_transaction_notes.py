from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("spaces", "0008_holiday_review_memory"),
    ]

    operations = [
        migrations.CreateModel(
            name="CounterpartyNote",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("counterparty", models.CharField(max_length=255)),
                ("counterparty_key", models.CharField(max_length=255, unique=True)),
                ("note", models.TextField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ("counterparty_key",),
            },
        ),
        migrations.CreateModel(
            name="TransactionNote",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("note", models.TextField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "feed_item",
                    models.OneToOneField(
                        on_delete=models.deletion.CASCADE,
                        related_name="transaction_note",
                        to="spaces.feeditem",
                    ),
                ),
            ],
            options={
                "ordering": ("feed_item_id",),
            },
        ),
    ]
