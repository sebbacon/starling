from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

from django.db.models import F, Sum, Min, Max, Q
from django.db.models.functions import TruncDay, TruncMonth, TruncWeek, Upper

from starling_web.spaces.models import FeedItem

EXCLUDED_TRANSFER_SOURCES = {"SAVINGS_GOAL", "INTERNAL_TRANSFER"}
MINOR_UNITS_QUANTUM = Decimal("1")


def calculate_spend_by_category(*, days: int, reference_time=None, start_time=None, spender=None):
    return _calculate_flow_by_category(
        days=days,
        reference_time=reference_time,
        start_time=start_time,
        flow="spending",
        spender=spender,
    )


def calculate_income_by_category(*, days: int, reference_time=None, start_time=None):
    return _calculate_flow_by_category(
        days=days,
        reference_time=reference_time,
        start_time=start_time,
        flow="income",
    )


def calculate_monthly_cashflow_totals(*, days: int, reference_time=None, start_time=None, income_scope: str = "salary"):
    if days <= 0:
        raise ValueError("days must be positive")
    if income_scope not in {"salary", "payments", "all"}:
        raise ValueError(f"Unsupported income scope: {income_scope}")

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

    base_queryset = _cashflow_base_queryset().filter(
        transaction_time__gte=window_start,
        transaction_time__lt=window_end,
    )

    spending_rows = (
        base_queryset.filter(amount_minor_units__lt=0)
        .annotate(period=TruncMonth("transaction_time"))
        .values("period")
        .annotate(total_minor=Sum(-F("amount_minor_units")))
        .order_by("period")
    )
    income_queryset = _apply_income_scope(base_queryset.filter(amount_minor_units__gt=0), income_scope)

    income_rows = (
        income_queryset
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
        "incomeScope": income_scope,
        "reference": reference.isoformat(),
        "days": days,
        "months": len(periods),
        "bucket": "month",
        "start": window_start.isoformat(),
        "end": window_end.isoformat(),
        "comparison": _calculate_cashflow_period_comparison(reference=reference, income_scope=income_scope),
    }


def _cashflow_base_queryset():
    return FeedItem.objects.annotate(source_upper=Upper("source")).exclude(source_upper__in=EXCLUDED_TRANSFER_SOURCES)


def _apply_income_scope(queryset, income_scope):
    if income_scope == "salary":
        return queryset.filter(Q(classified_category__icontains="salary"))
    if income_scope == "payments":
        return queryset.exclude(Q(classified_category__icontains="salary"))
    return queryset


def _round_minor_units(value):
    return int(Decimal(value).quantize(MINOR_UNITS_QUANTUM, rounding=ROUND_HALF_UP))


def _month_start(value):
    return datetime(value.year, value.month, 1, tzinfo=timezone.utc)


def _add_months(value, months):
    month_index = (value.year * 12) + (value.month - 1) + months
    year = month_index // 12
    month = (month_index % 12) + 1
    return datetime(year, month, 1, tzinfo=timezone.utc)


def _latest_complete_month_start(reference):
    current_month_start = _month_start(reference)
    if reference + timedelta(seconds=1) >= _add_months(current_month_start, 1):
        return current_month_start
    return _add_months(current_month_start, -1)


def _format_month_label(value):
    return value.strftime("%b %Y")


def _calculate_cashflow_period_comparison(*, reference, income_scope):
    current_end_month = _latest_complete_month_start(reference)
    current_start = _add_months(current_end_month, -11)
    current_end = _add_months(current_end_month, 1)
    previous_start = _add_months(current_start, -12)
    previous_end = current_start

    base_queryset = _cashflow_base_queryset()
    current_queryset = base_queryset.filter(transaction_time__gte=current_start, transaction_time__lt=current_end)
    previous_queryset = base_queryset.filter(transaction_time__gte=previous_start, transaction_time__lt=previous_end)

    current_spending = _monthly_total_map(current_queryset.filter(amount_minor_units__lt=0), -F("amount_minor_units"))
    previous_spending = _monthly_total_map(previous_queryset.filter(amount_minor_units__lt=0), -F("amount_minor_units"))
    current_income = _monthly_total_map(
        _apply_income_scope(current_queryset.filter(amount_minor_units__gt=0), income_scope),
        F("amount_minor_units"),
    )
    previous_income = _monthly_total_map(
        _apply_income_scope(previous_queryset.filter(amount_minor_units__gt=0), income_scope),
        F("amount_minor_units"),
    )

    return {
        "currentPeriod": _cashflow_period_payload(
            label=f"{_format_month_label(current_start)} to {_format_month_label(current_end_month)}",
            start=current_start,
            end=current_end,
            months=12,
            spending_map=current_spending,
            income_map=current_income,
        ),
        "previousPeriod": _cashflow_period_payload(
            label=f"{_format_month_label(previous_start)} to {_format_month_label(_add_months(previous_end, -1))}",
            start=previous_start,
            end=previous_end,
            months=12,
            spending_map=previous_spending,
            income_map=previous_income,
        ),
        "monthByMonth": [
            _cashflow_month_payload(
                current_month=_add_months(current_start, index),
                previous_month=_add_months(previous_start, index),
                current_spending=current_spending.get(_add_months(current_start, index).strftime("%Y-%m-%d"), 0),
                current_income=current_income.get(_add_months(current_start, index).strftime("%Y-%m-%d"), 0),
                previous_spending=previous_spending.get(_add_months(previous_start, index).strftime("%Y-%m-%d"), 0),
                previous_income=previous_income.get(_add_months(previous_start, index).strftime("%Y-%m-%d"), 0),
            )
            for index in range(12)
        ],
    }


def _monthly_total_map(queryset, total_expression):
    rows = (
        queryset.annotate(period=TruncMonth("transaction_time"))
        .values("period")
        .annotate(total_minor=Sum(total_expression))
        .order_by("period")
    )
    return {
        row["period"].strftime("%Y-%m-%d"): int(row["total_minor"] or 0)
        for row in rows
        if row["period"] is not None
    }


def _cashflow_period_payload(*, label, start, end, months, spending_map, income_map):
    month_keys = [_add_months(start, index).strftime("%Y-%m-%d") for index in range(months)]
    spending_minor = sum(spending_map.get(key, 0) for key in month_keys)
    income_minor = sum(income_map.get(key, 0) for key in month_keys)
    net_minor = income_minor - spending_minor
    average_spending_minor = _round_minor_units(Decimal(spending_minor) / Decimal(months))
    average_income_minor = _round_minor_units(Decimal(income_minor) / Decimal(months))
    average_net_minor = _round_minor_units(Decimal(net_minor) / Decimal(months))

    return {
        "label": label,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "months": months,
        "spendingMinorUnits": spending_minor,
        "spending": round(spending_minor / 100, 2),
        "incomeMinorUnits": income_minor,
        "income": round(income_minor / 100, 2),
        "netMinorUnits": net_minor,
        "net": round(net_minor / 100, 2),
        "averageMonthlySpendingMinorUnits": average_spending_minor,
        "averageMonthlySpending": round(average_spending_minor / 100, 2),
        "averageMonthlyIncomeMinorUnits": average_income_minor,
        "averageMonthlyIncome": round(average_income_minor / 100, 2),
        "averageMonthlyNetMinorUnits": average_net_minor,
        "averageMonthlyNet": round(average_net_minor / 100, 2),
    }


def _cashflow_month_payload(*, current_month, previous_month, current_spending, current_income, previous_spending, previous_income):
    current_net = current_income - current_spending
    previous_net = previous_income - previous_spending
    return {
        "label": _format_month_label(current_month),
        "previousLabel": _format_month_label(previous_month),
        "currentPeriodSpendingMinorUnits": current_spending,
        "currentPeriodSpending": round(current_spending / 100, 2),
        "currentPeriodIncomeMinorUnits": current_income,
        "currentPeriodIncome": round(current_income / 100, 2),
        "currentPeriodNetMinorUnits": current_net,
        "currentPeriodNet": round(current_net / 100, 2),
        "previousPeriodSpendingMinorUnits": previous_spending,
        "previousPeriodSpending": round(previous_spending / 100, 2),
        "previousPeriodIncomeMinorUnits": previous_income,
        "previousPeriodIncome": round(previous_income / 100, 2),
        "previousPeriodNetMinorUnits": previous_net,
        "previousPeriodNet": round(previous_net / 100, 2),
    }


def _calculate_flow_by_category(*, days: int, reference_time=None, start_time=None, flow: str, spender=None):
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
    )
    if spender:
        qs = qs.filter(spender__iexact=spender)
    summary_rows = (
        qs.values("period", "classified_category", "currency")
        .annotate(total_minor=Sum(amount_total_expression))
        .order_by("period", "classified_category")
    )
    spender_rows = (
        qs.values("period", "classified_category", "currency", "spender")
        .annotate(total_minor=Sum(amount_total_expression))
        .order_by("period", "spender", "classified_category")
    )

    periods = sorted({format_period(row["period"]) for row in summary_rows if row["period"]})
    dates = periods

    series_accumulator = {}
    for row in summary_rows:
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

    spender_series_accumulator = {"seb": {}, "kim": {}}
    for row in spender_rows:
        period_value = row["period"]
        if period_value is None:
            continue
        spender_key = (row["spender"] or "").strip().lower()
        if spender_key not in spender_series_accumulator:
            continue
        period_key = format_period(period_value)
        category = row["classified_category"] or "Uncategorised"
        currency = row["currency"] or "GBP"
        spender_series_accumulator[spender_key].setdefault(
            category,
            {
                "currency": currency,
                "values": {date: 0 for date in dates},
            },
        )
        spender_series_accumulator[spender_key][category]["values"][period_key] = int(row["total_minor"] or 0)

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

    spender_series = {"seb": [], "kim": []}
    for spender_key, categories in spender_series_accumulator.items():
        for category, payload in sorted(categories.items()):
            ordered_minor = [payload["values"][date] for date in dates]
            total_minor = sum(ordered_minor)
            month_count = len(ordered_minor) if ordered_minor else 0
            avg_minor = int(total_minor / month_count) if month_count else 0
            spender_series[spender_key].append(
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
        "spenderSeries": spender_series,
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
