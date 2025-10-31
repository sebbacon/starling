from datetime import datetime, timedelta, timezone

from django.db.models import F, Sum
from django.db.models.functions import TruncDay, TruncMonth, TruncWeek, Upper

from starling_web.spaces.models import FeedItem

EXCLUDED_TRANSFER_SOURCES = {"SAVINGS_GOAL", "INTERNAL_TRANSFER"}


def calculate_spend_by_category(*, days: int, reference_time=None, start_time=None):
    if days <= 0:
        raise ValueError("days must be positive")

    reference = reference_time or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    else:
        reference = reference.astimezone(timezone.utc)

    window_start = reference - timedelta(days=days)
    window_end = reference + timedelta(seconds=1)
    if start_time is not None:
        if start_time.tzinfo is None:
            window_start = start_time.replace(tzinfo=timezone.utc)
        else:
            window_start = start_time.astimezone(timezone.utc)

    if days <= 35:
        trunc = TruncDay
        bucket = "day"
        format_period = lambda dt: dt.strftime("%Y-%m-%d")
    elif days <= 210:
        trunc = TruncWeek
        bucket = "week"
        format_period = lambda dt: dt.strftime("%Y-%m-%d")
    else:
        trunc = TruncMonth
        bucket = "month"
        format_period = lambda dt: dt.strftime("%Y-%m-%d")

    qs = (
        FeedItem.objects.filter(
            transaction_time__gte=window_start,
            transaction_time__lt=window_end,
            amount_minor_units__lt=0,
        )
        .annotate(period=trunc("transaction_time"), source_upper=Upper("source"))
        .exclude(source_upper__in=EXCLUDED_TRANSFER_SOURCES)
        .values("period", "classified_category", "currency")
        .annotate(total_minor=Sum(-F("amount_minor_units")))
        .order_by("period", "classified_category")
    )

    periods = sorted({format_period(row["period"]) for row in qs if row["period"]})
    dates = periods

    series_accumulator = {}
    for row in qs:
        period_value = row["period"]
        if period_value is None:
            continue
        period_key = format_period(period_value)
        category = row["classified_category"] or "Uncategorised"
        currency = row["currency"] or "GBP"
        series_accumulator.setdefault(
            category,
            {
                "currency": currency,
                "values": {date: 0 for date in dates},
            },
        )
        series_accumulator[category]["values"][period_key] = int(row["total_minor"] or 0)

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
        "bucket": bucket,
        "start": window_start.isoformat(),
        "end": window_end.isoformat(),
    }
