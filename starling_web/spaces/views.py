from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

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


def _wants_json(request):
    accept_header = request.META.get("HTTP_ACCEPT", "")
    return "application/json" in accept_header
