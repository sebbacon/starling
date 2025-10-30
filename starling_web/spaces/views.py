from datetime import datetime, timezone

from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from starling_spaces.analytics import calculate_spend_by_category
from starling_spaces.ingestion import calculate_average_spend


def _build_summary():
    return calculate_average_spend(
        db_path=settings.STARLING_FEEDS_DB,
        days=settings.STARLING_SUMMARY_DAYS,
    )


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
def spending(request):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        days = _parse_positive_int(request.GET.get("days"), default_days)
    except ValueError:
        days = default_days

    return render(
        request,
        "spaces/spending.html",
        {"summary_days": days},
    )


@require_GET
def spending_data(request):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        days = _parse_positive_int(request.GET.get("days"), default_days)
        reference = _parse_reference_time(request.GET.get("reference"))
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    summary = calculate_spend_by_category(
        db_path=settings.STARLING_FEEDS_DB,
        days=days,
        reference_time=reference,
    )
    return JsonResponse(summary)


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
