from datetime import datetime, timedelta, timezone

from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.views.decorators.http import require_GET
from django.db.models.functions import Upper

from starling_spaces.analytics import (
    calculate_spend_by_category,
    EXCLUDED_TRANSFER_SOURCES,
)
from starling_spaces.ingestion import calculate_average_spend
from starling_web.spaces.models import Category, FeedItem


def _build_summary():
    return calculate_average_spend(days=settings.STARLING_SUMMARY_DAYS)


@require_GET
def home(request):
    context = {
        "summary": _build_summary(),
        "summary_days": settings.STARLING_SUMMARY_DAYS,
    }
    return render(request, "spaces/home.html", context)


@require_GET
def summary(request):
    summary_payload = _build_summary()

    if _wants_json(request):
        return JsonResponse(summary_payload)

    return render(
        request,
        "spaces/_summary.html",
        {"summary": summary_payload, "summary_days": settings.STARLING_SUMMARY_DAYS},
    )


@require_GET
def spending(request, category_name=None, counterparty_name=None):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        days = _parse_positive_int(request.GET.get("days"), default_days)
    except ValueError:
        days = default_days

    counterparty_template = reverse("spaces:spending-counterparty", args=["__counterparty__"])
    counterparty_base = counterparty_template.rsplit("__counterparty__", 1)[0]

    return render(
        request,
        "spaces/spending.html",
        {
            "summary_days": days,
            "initial_category": category_name or "",
            "initial_counterparty": counterparty_name or "",
            "base_spending_url": reverse("spaces:spending"),
            "counterparty_base_url": counterparty_base,
        },
    )


@require_GET
def spending_data(request):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        days = _parse_positive_int(request.GET.get("days"), default_days)
        reference = _parse_reference_time(request.GET.get("reference"))
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    summary = calculate_spend_by_category(days=days, reference_time=reference)
    return JsonResponse(summary)


@require_GET
def spending_transactions(request):
    category = request.GET.get("category")
    counterparty = request.GET.get("counterparty")
    if not category and not counterparty:
        return JsonResponse({"error": "category or counterparty is required"}, status=400)

    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        days = _parse_positive_int(request.GET.get("days"), default_days)
        reference = _parse_reference_time(request.GET.get("reference"))
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    reference_time = reference or datetime.now(timezone.utc)
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=timezone.utc)
    else:
        reference_time = reference_time.astimezone(timezone.utc)

    window_start = reference_time - timedelta(days=days)
    window_end = reference_time + timedelta(seconds=1)

    queryset = (
        FeedItem.objects.filter(
            transaction_time__gte=window_start,
            transaction_time__lt=window_end,
        )
        .annotate(source_upper=Upper("source"))
        .exclude(source_upper__in=EXCLUDED_TRANSFER_SOURCES)
        .order_by("-transaction_time")
    )

    if category:
        queryset = queryset.filter(amount_minor_units__lt=0)
        if category == "Uncategorised":
            queryset = queryset.filter(classified_category__isnull=True)
        else:
            queryset = queryset.filter(classified_category=category)

    if counterparty:
        queryset = queryset.filter(counterparty=counterparty)

    space_names = {
        (cat.account_uid, cat.category_uid): cat.name
        for cat in Category.objects.filter(category_type="space")
    }

    transactions = []
    for item in queryset:
        space_name = space_names.get((item.account_uid, item.space_uid))
        transactions.append(
            {
                "feedItemUid": item.feed_item_uid,
                "transactionTime": item.transaction_time.isoformat(),
                "counterparty": item.counterparty or "",
                "amountMinorUnits": int(-item.amount_minor_units),
                "currency": item.currency or "GBP",
                "spaceUid": item.space_uid or "",
                "spaceName": space_name or "",
                "source": item.source or "",
                "classificationReason": item.classification_reason or "",
                "category": item.classified_category or "Uncategorised",
                "raw": item.raw_json or {},
            }
        )

    response = {
        "reference": reference_time.isoformat(),
        "days": days,
        "count": len(transactions),
        "transactions": transactions,
    }
    if category:
        response["category"] = category
    if counterparty:
        response["counterparty"] = counterparty
    return JsonResponse(response)


def _wants_json(request):
    accept_header = request.META.get("HTTP_ACCEPT", "")
    return "application/json" in accept_header


def _parse_positive_int(value, default):
    if not value:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer value: {value}") from exc
    if parsed <= 0:
        raise ValueError("Value must be positive")
    return parsed


def _parse_reference_time(value):
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError(f"Invalid reference time: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed
