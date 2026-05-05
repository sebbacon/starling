from django.contrib import admin
from django.db.models import Q

from .models import (
    ApplicationUser,
    Category,
    ClassificationRule,
    CounterpartyNote,
    FeedItem,
    HolidayMerchantOverride,
    HolidaySuggestionDecision,
    SavingsSignalDismissal,
    SyncState,
    TransactionNote,
    UserDefinedCategory,
)


@admin.register(UserDefinedCategory)
class UserDefinedCategoryAdmin(admin.ModelAdmin):
    list_display = ("name",)
    search_fields = ("name",)


@admin.register(ApplicationUser)
class ApplicationUserAdmin(admin.ModelAdmin):
    list_display = ("user_uid", "name")
    search_fields = ("user_uid", "name")


@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = ("account_uid", "category_type", "category_uid", "name")
    list_filter = ("category_type",)
    search_fields = ("account_uid", "category_uid", "name")


@admin.register(CounterpartyNote)
class CounterpartyNoteAdmin(admin.ModelAdmin):
    list_display = ("counterparty", "updated_at")
    search_fields = ("counterparty", "note")
    ordering = ("counterparty",)


class FeedItemCategoryStateFilter(admin.SimpleListFilter):
    title = "category state"
    parameter_name = "category_state"

    def lookups(self, request, model_admin):
        return (
            ("uncategorised", "Uncategorised (null/blank/literal)"),
            ("null", "NULL only"),
            ("blank", "Blank only"),
            ("literal_uncategorised", 'Literal "Uncategorised"'),
            ("categorised", "Categorised"),
        )

    def queryset(self, request, queryset):
        value = self.value()
        uncategorised_q = (
            Q(classified_category__isnull=True)
            | Q(classified_category="")
            | Q(classified_category__iexact="Uncategorised")
        )
        if value == "uncategorised":
            return queryset.filter(uncategorised_q)
        if value == "null":
            return queryset.filter(classified_category__isnull=True)
        if value == "blank":
            return queryset.filter(classified_category="")
        if value == "literal_uncategorised":
            return queryset.filter(classified_category__iexact="Uncategorised")
        if value == "categorised":
            return queryset.exclude(uncategorised_q)
        return queryset


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
    list_filter = (
        FeedItemCategoryStateFilter,
        "currency",
        "counterparty",
        "classified_category",
        "classification_reason",
        "source",
    )
    search_fields = ("feed_item_uid", "counterparty", "classified_category")
    ordering = ("-transaction_time",)


@admin.register(SyncState)
class SyncStateAdmin(admin.ModelAdmin):
    list_display = ("account_uid", "category_uid", "last_transaction_time")
    search_fields = ("account_uid", "category_uid")


@admin.register(TransactionNote)
class TransactionNoteAdmin(admin.ModelAdmin):
    list_display = ("feed_item", "updated_at")
    search_fields = ("feed_item__feed_item_uid", "note")
    ordering = ("feed_item__transaction_time",)


@admin.register(SavingsSignalDismissal)
class SavingsSignalDismissalAdmin(admin.ModelAdmin):
    list_display = ("signal_type", "signal_key", "label", "updated_at")
    list_filter = ("signal_type",)
    search_fields = ("signal_type", "signal_key", "label")
    ordering = ("signal_type", "signal_key")


@admin.register(ClassificationRule)
class ClassificationRuleAdmin(admin.ModelAdmin):
    list_display = (
        "position",
        "rule_type",
        "category",
        "reason",
        "pattern",
        "space_uid",
        "amount_bounds_display",
        "start_date",
        "end_date",
    )
    list_filter = ("rule_type",)
    search_fields = ("category", "reason", "pattern", "space_uid")
    ordering = ("position", "id")


@admin.register(HolidayMerchantOverride)
class HolidayMerchantOverrideAdmin(admin.ModelAdmin):
    list_display = ("merchant_key", "label", "override_type", "updated_at")
    list_filter = ("override_type",)
    search_fields = ("merchant_key", "label")
    ordering = ("merchant_key",)


@admin.register(HolidaySuggestionDecision)
class HolidaySuggestionDecisionAdmin(admin.ModelAdmin):
    list_display = ("feed_item_uid", "decision", "updated_at")
    list_filter = ("decision",)
    search_fields = ("feed_item_uid",)
    ordering = ("feed_item_uid",)
