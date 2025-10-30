from datetime import datetime, timedelta, timezone

from django.db.models import F, Sum
from django.db.models.functions import TruncMonth, Upper

from starling_web.spaces.models import FeedItem

EXCLUDED_TRANSFER_SOURCES = {"SAVINGS_GOAL", "INTERNAL_TRANSFER"}


def calculate_spend_by_category(*, days: int, reference_time=None):
    if days <= 0:
        raise ValueError("days must be positive")

    reference = reference_time or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    else:
        reference = reference.astimezone(timezone.utc)

    window_start = reference - timedelta(days=days)
    window_end = reference + timedelta(seconds=1)

    qs = (
        FeedItem.objects.filter(
            transaction_time__gte=window_start,
            transaction_time__lt=window_end,
            amount_minor_units__lt=0,
        )
        .annotate(month=TruncMonth("transaction_time"), source_upper=Upper("source"))
        .exclude(source_upper__in=EXCLUDED_TRANSFER_SOURCES)
        .values("month", "classified_category", "currency")
        .annotate(total_minor=Sum(-F("amount_minor_units")))
        .order_by("month", "classified_category")
    )

    months = sorted({row["month"].strftime("%Y-%m") for row in qs if row["month"]})
    dates = [f"{month}-01" for month in months]

    series_accumulator = {}
    for row in qs:
        month_key = row["month"].strftime("%Y-%m-01") if row["month"] else None
        if month_key is None:
            continue
        category = row["classified_category"] or "Uncategorised"
        currency = row["currency"] or "GBP"
        series_accumulator.setdefault(
            category,
            {
                "currency": currency,
                "values": {date: 0 for date in dates},
            },
        )
        series_accumulator[category]["values"][month_key] = int(row["total_minor"] or 0)

    series = []
    for category, payload in sorted(series_accumulator.items()):
        ordered_minor = [payload["values"][date] for date in dates]
        total_minor = sum(ordered_minor)
        month_count = len(ordered_minor) if ordered_minor else 0
        avg_minor = int(total_minor / month_count) if month_count else 0
        series.append(
            {
                "category": category,
                "values": [round(value / 100, 2) for value in ordered_minor],
                "minorValues": ordered_minor,
                "totalMinorUnits": total_minor,
                "currency": payload["currency"],
                "averageMinorUnits": avg_minor,
                "average": round(avg_minor / 100, 2),
            }
        )

    return {
        "dates": dates,
        "series": series,
        "reference": reference.isoformat(),
        "days": days,
        "months": len(dates),
    }
