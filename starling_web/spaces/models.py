from django.db import models


class Category(models.Model):
    account_uid = models.CharField(max_length=64)
    category_type = models.CharField(max_length=16)
    category_uid = models.CharField(max_length=64)
    space_uid = models.CharField(max_length=64, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        unique_together = ("account_uid", "category_type", "category_uid")
        indexes = [
            models.Index(fields=["account_uid", "category_type", "category_uid"]),
        ]

    def __str__(self):
        return self.name or f"{self.category_type}:{self.category_uid}"


class ClassificationRule(models.Model):
    position = models.PositiveIntegerField(unique=True)
    rule_type = models.CharField(max_length=32)
    category = models.CharField(max_length=255, blank=True, null=True)
    reason = models.CharField(max_length=128, blank=True, null=True)
    pattern = models.CharField(max_length=255, blank=True, null=True)
    space_uid = models.CharField(max_length=64, blank=True, null=True)
    json_path = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        ordering = ("position", "id")

    def __str__(self):
        label = self.reason or self.rule_type
        return f"{self.position}: {label}"


class FeedItem(models.Model):
    feed_item_uid = models.CharField(primary_key=True, max_length=64)
    account_uid = models.CharField(max_length=64)
    category_uid = models.CharField(max_length=64)
    space_uid = models.CharField(max_length=64, blank=True, null=True)
    direction = models.CharField(max_length=16, blank=True, null=True)
    amount_minor_units = models.BigIntegerField()
    currency = models.CharField(max_length=8)
    transaction_time = models.DateTimeField()
    source = models.CharField(max_length=64, blank=True, null=True)
    counterparty = models.CharField(max_length=255, blank=True, null=True)
    spending_category = models.CharField(max_length=64, blank=True, null=True)
    classified_category = models.CharField(max_length=255, blank=True, null=True)
    classification_reason = models.CharField(max_length=64, blank=True, null=True)
    raw_json = models.JSONField(default=dict)

    class Meta:
        indexes = [
            models.Index(fields=["account_uid", "space_uid", "transaction_time"]),
            models.Index(fields=["transaction_time"]),
        ]

    def __str__(self):
        return self.feed_item_uid


class SyncState(models.Model):
    account_uid = models.CharField(max_length=64)
    category_uid = models.CharField(max_length=64)
    last_transaction_time = models.CharField(max_length=64, blank=True, null=True)

    class Meta:
        unique_together = ("account_uid", "category_uid")

    def __str__(self):
        return f"{self.account_uid}:{self.category_uid}"
