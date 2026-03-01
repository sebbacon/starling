from datetime import datetime, timedelta, timezone

from django.db.models import F, Sum, Min, Max
from django.db.models.functions import TruncDay, TruncMonth, TruncWeek, Upper

from starling_web.spaces.models import FeedItem

EXCLUDED_TRANSFER_SOURCES = {"SAVINGS_GOAL", "INTERNAL_TRANSFER"}


def calculate_spend_by_category(*, days: int, reference_time=None, start_time=None):
    return _calculate_flow_by_category(
        days=days,
        reference_time=reference_time,
        start_time=start_time,
        flow="spending",
    )


def calculate_income_by_category(*, days: int, reference_time=None, start_time=None):
    return _calculate_flow_by_category(
        days=days,
        reference_time=reference_time,
        start_time=start_time,
        flow="income",
    )


def calculate_monthly_cashflow_totals(*, days: int, reference_time=None, start_time=None):
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

    base_queryset = (
        FeedItem.objects.filter(
            transaction_time__gte=window_start,
            transaction_time__lt=window_end,
        )
        .annotate(source_upper=Upper("source"))
        .exclude(source_upper__in=EXCLUDED_TRANSFER_SOURCES)
    )

    spending_rows = (
        base_queryset.filter(amount_minor_units__lt=0)
        .annotate(period=TruncMonth("transaction_time"))
        .values("period")
        .annotate(total_minor=Sum(-F("amount_minor_units")))
        .order_by("period")
    )
    income_rows = (
        base_queryset.filter(amount_minor_units__gt=0)
        .annotate(period=TruncMonth("transaction_time"))
        .values("period")
        .annotate(total_minor=Sum(F("amount_minor_units")))
        .order_by("period")
    )

    spending_map = {
        row["period"].strftime("%Y-%m-%d"): int(row["total_minor"] or 0)
        for row in spending_rows
        if row["period"] is not None
    }
    income_map = {
        row["period"].strftime("%Y-%m-%d"): int(row["total_minor"] or 0)
        for row in income_rows
        if row["period"] is not None
    }
    periods = sorted(set(spending_map) | set(income_map))

    spending_minor_values = [spending_map.get(period, 0) for period in periods]
    income_minor_values = [income_map.get(period, 0) for period in periods]

    return {
        "dates": periods,
        "spendingValues": [round(value / 100, 2) for value in spending_minor_values],
        "incomeValues": [round(value / 100, 2) for value in income_minor_values],
        "spendingMinorValues": spending_minor_values,
        "incomeMinorValues": income_minor_values,
        "reference": reference.isoformat(),
        "days": days,
        "months": len(periods),
        "bucket": "month",
        "start": window_start.isoformat(),
        "end": window_end.isoformat(),
    }


def _calculate_flow_by_category(*, days: int, reference_time=None, start_time=None, flow: str):
    if days <= 0:
        raise ValueError("days must be positive")
    if flow not in {"spending", "income"}:
        raise ValueError(f"Unsupported flow: {flow}")

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

    if flow == "spending":
        amount_filter = {"amount_minor_units__lt": 0}
        amount_total_expression = -F("amount_minor_units")
    else:
        amount_filter = {"amount_minor_units__gt": 0}
        amount_total_expression = F("amount_minor_units")

    qs = (
        FeedItem.objects.filter(
            transaction_time__gte=window_start,
            transaction_time__lt=window_end,
            **amount_filter,
        )
        .annotate(period=trunc("transaction_time"), source_upper=Upper("source"))
        .exclude(source_upper__in=EXCLUDED_TRANSFER_SOURCES)
        .values("period", "classified_category", "currency")
        .annotate(total_minor=Sum(amount_total_expression))
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


def summarise_category_totals(*, period: str, reference_time=None):
    if period not in {"past_month", "all_time", "monthly_average"}:
        raise ValueError(f"Unsupported period: {period}")

    reference = reference_time or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    else:
        reference = reference.astimezone(timezone.utc)

    end = reference + timedelta(seconds=1)

    base_queryset = (
        FeedItem.objects.filter(
            amount_minor_units__lt=0,
            transaction_time__lt=end,
        )
        .annotate(source_upper=Upper("source"))
        .exclude(source_upper__in=EXCLUDED_TRANSFER_SOURCES)
    )

    if period == "past_month":
        start = reference - timedelta(days=30)
        queryset = base_queryset.filter(transaction_time__gte=start)
        months_count = 0
    else:
        bounds = base_queryset.aggregate(
            earliest=Min("transaction_time"),
            latest=Max("transaction_time"),
        )
        earliest = bounds["earliest"]
        latest = bounds["latest"]
        if earliest is None or latest is None:
            return {
                "period": period,
                "reference": reference,
                "start": None,
                "end": end,
                "categories": [],
                "months": 0,
            }
        start = earliest
        queryset = base_queryset.filter(transaction_time__gte=start)
        months_count = ((latest.year - earliest.year) * 12) + (latest.month - earliest.month) + 1

    rows = (
        queryset.values("classified_category")
        .annotate(total_minor=Sum(-F("amount_minor_units")))
        .order_by()
    )

    categories = []
    for row in rows:
        category_name = row["classified_category"] or "Uncategorised"
        total_minor = int(row["total_minor"] or 0)
        if total_minor <= 0:
            continue
        categories.append((category_name, total_minor))

    categories.sort(key=lambda item: item[1], reverse=True)

    return {
        "period": period,
        "reference": reference,
        "start": start,
        "end": end,
        "categories": categories,
        "months": months_count,
    }
