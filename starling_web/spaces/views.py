from datetime import datetime, timedelta, timezone
import math
from decimal import Decimal, InvalidOperation

from django.conf import settings
from django.db.models import Q
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.views.decorators.http import require_GET
from django.db.models.functions import Upper

from starling_spaces.analytics import (
    calculate_spend_by_category,
    EXCLUDED_TRANSFER_SOURCES,
)
from starling_web.spaces.models import Category, FeedItem


@require_GET
def spending(request, category_name=None, counterparty_name=None):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        _, _, days, _ = _resolve_time_window(request, default_days)
    except ValueError:
        days = default_days

    search_query = (request.GET.get("search") or "").strip()

    counterparty_template = reverse("spaces:spending-counterparty", args=["__counterparty__"])
    counterparty_base = counterparty_template.rsplit("__counterparty__", 1)[0]

    return render(
        request,
        "spaces/spending.html",
        {
            "summary_days": days,
            "initial_category": category_name or "",
            "initial_counterparty": counterparty_name or "",
            "initial_search": search_query,
            "base_spending_url": reverse("spaces:spending"),
            "counterparty_base_url": counterparty_base,
        },
    )


@require_GET
def spending_data(request):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        window_start, window_end, days, reference = _resolve_time_window(request, default_days)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    summary = calculate_spend_by_category(
        days=days,
        reference_time=reference,
        start_time=window_start,
    )
    return JsonResponse(summary)


@require_GET
def spending_transactions(request):
    category = request.GET.get("category")
    counterparty = request.GET.get("counterparty")
    search = (request.GET.get("search") or "").strip()
    if not category and not counterparty:
        if not search:
            return JsonResponse({"error": "category, counterparty, or search is required"}, status=400)

    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        window_start, window_end, days, reference = _resolve_time_window(request, default_days)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)
    reference_time = reference or datetime.now(timezone.utc)

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
        queryset = queryset.filter(counterparty__iexact=counterparty)

    if search:
        amount_minor = _parse_amount_minor_units(search)
        search_filter = Q(counterparty__icontains=search)
        if amount_minor is not None:
            search_filter |= Q(amount_minor_units=-amount_minor) | Q(amount_minor_units=amount_minor)
        queryset = queryset.filter(search_filter)

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
        "start": window_start.isoformat(),
        "end": window_end.isoformat(),
        "count": len(transactions),
        "transactions": transactions,
    }
    if category:
        response["category"] = category
    if counterparty:
        response["counterparty"] = counterparty
    if search:
        response["search"] = search
    return JsonResponse(response)


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
    return _normalize_to_utc(parsed)


def _parse_amount_minor_units(value):
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    cleaned = cleaned.replace(",", "")
    if cleaned.startswith("£"):
        cleaned = cleaned[1:]
    if not cleaned:
        return None
    try:
        amount = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None
    try:
        quantised = amount.quantize(Decimal("0.01"))
    except InvalidOperation:
        return None
    minor_units = int(abs(quantised * 100))
    return minor_units


def _normalize_to_utc(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _resolve_time_window(request, default_days):
    start_raw = request.GET.get("start")
    end_raw = request.GET.get("end")

    if start_raw or end_raw:
        if not start_raw or not end_raw:
            raise ValueError("start and end must be provided together")
        start = _parse_reference_time(start_raw)
        end = _parse_reference_time(end_raw)
        if start is None or end is None:
            raise ValueError("Invalid date range provided")
        start = _normalize_to_utc(start)
        end = _normalize_to_utc(end)
        if end <= start:
            raise ValueError("end must be after start")
        seconds = (end - start).total_seconds()
        days = max(1, math.ceil(seconds / 86400))
        reference_time = end - timedelta(seconds=1)
        return start, end, days, reference_time

    days = _parse_positive_int(request.GET.get("days"), default_days)
    reference = _parse_reference_time(request.GET.get("reference"))
    if reference is None:
        reference = datetime.now(timezone.utc)
    else:
        reference = _normalize_to_utc(reference)
    start = reference - timedelta(days=days)
    end = reference + timedelta(seconds=1)
    return start, end, days, reference
