from django.contrib import admin

from .models import Category, FeedItem, SyncState


@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = ("account_uid", "category_type", "category_uid", "name")
    list_filter = ("category_type",)
    search_fields = ("account_uid", "category_uid", "name")


@admin.register(FeedItem)
class FeedItemAdmin(admin.ModelAdmin):
    list_display = (
        "feed_item_uid",
        "account_uid",
        "space_uid",
        "counterparty",
        "classified_category",
        "amount_minor_units",
        "transaction_time",
    )
    list_filter = ("currency", "counterparty", "classified_category", "classification_reason", "source")
    search_fields = ("feed_item_uid", "counterparty", "classified_category")
    ordering = ("-transaction_time",)


@admin.register(SyncState)
class SyncStateAdmin(admin.ModelAdmin):
    list_display = ("account_uid", "category_uid", "last_transaction_time")
    search_fields = ("account_uid", "category_uid")

# Register your models here.
