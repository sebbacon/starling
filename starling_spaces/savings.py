import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median

from django.db.models.functions import Upper

from starling_web.spaces.models import FeedItem

from .analytics import EXCLUDED_TRANSFER_SOURCES


CONFIDENCE_PROFILES = {
    "high": {
        "subscription_min_occurrences": 4,
        "subscription_min_amount_minor": 700,
        "subscription_interval_match_min": 0.7,
        "subscription_amount_match_min": 0.7,
        "subscription_amount_variation_ratio": 0.1,
        "trend_prev_average_minor_min": 5000,
        "trend_delta_minor_min": 3000,
        "trend_delta_ratio_min": 0.25,
        "anomaly_min_history": 5,
        "anomaly_min_amount_minor": 5000,
        "anomaly_mad_multiplier": 4.0,
        "anomaly_min_lift_minor": 4000,
        "anomaly_singleton_min_amount_minor": 9000,
        "impact_scale_minor": 12000,
        "max_signals": 25,
    },
    "balanced": {
        "subscription_min_occurrences": 3,
        "subscription_min_amount_minor": 500,
        "subscription_interval_match_min": 0.6,
        "subscription_amount_match_min": 0.6,
        "subscription_amount_variation_ratio": 0.15,
        "trend_prev_average_minor_min": 3000,
        "trend_delta_minor_min": 2000,
        "trend_delta_ratio_min": 0.18,
        "anomaly_min_history": 4,
        "anomaly_min_amount_minor": 3500,
        "anomaly_mad_multiplier": 3.5,
        "anomaly_min_lift_minor": 2500,
        "anomaly_singleton_min_amount_minor": 7000,
        "impact_scale_minor": 9000,
        "max_signals": 40,
    },
    "broad": {
        "subscription_min_occurrences": 3,
        "subscription_min_amount_minor": 300,
        "subscription_interval_match_min": 0.5,
        "subscription_amount_match_min": 0.5,
        "subscription_amount_variation_ratio": 0.2,
        "trend_prev_average_minor_min": 2000,
        "trend_delta_minor_min": 1000,
        "trend_delta_ratio_min": 0.1,
        "anomaly_min_history": 3,
        "anomaly_min_amount_minor": 2500,
        "anomaly_mad_multiplier": 3.0,
        "anomaly_min_lift_minor": 1500,
        "anomaly_singleton_min_amount_minor": 5000,
        "impact_scale_minor": 7000,
        "max_signals": 60,
    },
}

GROUP_TYPES = {
    "subscriptions": "subscription",
    "trends": "trend",
    "anomalies": "anomaly",
}


def calculate_savings_signals(*, days: int, reference_time=None, start_time=None, confidence_mode="balanced", group="all"):
    if days <= 0:
        raise ValueError("days must be positive")
    if confidence_mode not in CONFIDENCE_PROFILES:
        raise ValueError(f"Unsupported confidence mode: {confidence_mode}")
    if group not in {"all", "subscriptions", "trends", "anomalies"}:
        raise ValueError(f"Unsupported group: {group}")

    reference = _normalize_to_utc(reference_time or datetime.now(timezone.utc))
    window_start = reference - timedelta(days=days)
    if start_time is not None:
        window_start = _normalize_to_utc(start_time)
    window_end = reference + timedelta(seconds=1)

    profile = CONFIDENCE_PROFILES[confidence_mode]
    transactions = _load_spending_transactions(window_start=window_start, window_end=window_end)

    signals = []
    signals.extend(
        _build_subscription_signals(
            transactions=transactions,
            reference=reference,
            profile=profile,
        )
    )
    signals.extend(
        _build_trend_signals(
            transactions=transactions,
            window_start=window_start,
            reference=reference,
            profile=profile,
        )
    )
    signals.extend(
        _build_anomaly_signals(
            transactions=transactions,
            reference=reference,
            profile=profile,
        )
    )

    if group != "all":
        selected_type = GROUP_TYPES[group]
        signals = [signal for signal in signals if signal["type"] == selected_type]

    signals.sort(
        key=lambda item: (
            -item["priorityScore"],
            -item["confidenceScore"],
            -item["impactMonthlyMinor"],
            item["title"],
        )
    )
    max_signals = profile["max_signals"]
    if len(signals) > max_signals:
        signals = signals[:max_signals]

    potential_monthly_minor = sum(max(0, int(item["impactMonthlyMinor"])) for item in signals)
    potential_annual_minor = sum(max(0, int(item["impactAnnualMinor"])) for item in signals)
    high_confidence_count = sum(1 for item in signals if item["confidenceScore"] >= 75)

    return {
        "summary": {
            "potentialMonthlySavingsMinor": potential_monthly_minor,
            "potentialAnnualSavingsMinor": potential_annual_minor,
            "signalsCount": len(signals),
            "highConfidenceCount": high_confidence_count,
        },
        "signals": signals,
        "window": {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
            "days": days,
        },
        "confidenceMode": confidence_mode,
        "group": group,
    }


def _load_spending_transactions(*, window_start, window_end):
    rows = (
        FeedItem.objects.filter(
            transaction_time__gte=window_start,
            transaction_time__lt=window_end,
            amount_minor_units__lt=0,
        )
        .annotate(source_upper=Upper("source"))
        .exclude(source_upper__in=EXCLUDED_TRANSFER_SOURCES)
        .values(
            "feed_item_uid",
            "transaction_time",
            "counterparty",
            "classified_category",
            "amount_minor_units",
            "currency",
            "source",
        )
        .order_by("transaction_time", "feed_item_uid")
    )
    transactions = []
    for row in rows:
        timestamp = _normalize_to_utc(row["transaction_time"])
        counterparty = (row["counterparty"] or "").strip()
        category = (row["classified_category"] or "").strip() or "Uncategorised"
        source = (row["source"] or "").strip()
        transactions.append(
            {
                "feedItemUid": row["feed_item_uid"],
                "transactionTime": timestamp,
                "counterparty": counterparty,
                "category": category,
                "amountMinor": int(-(row["amount_minor_units"] or 0)),
                "currency": (row["currency"] or "GBP").strip() or "GBP",
                "source": source,
            }
        )
    return transactions


def _build_subscription_signals(*, transactions, reference, profile):
    grouped = defaultdict(list)
    for item in transactions:
        counterparty = item["counterparty"]
        if not counterparty:
            continue
        grouped[counterparty].append(item)

    signals = []
    for counterparty, items in grouped.items():
        items.sort(key=lambda row: row["transactionTime"])
        if len(items) < profile["subscription_min_occurrences"]:
            continue

        intervals = []
        for index in range(1, len(items)):
            days_delta = (items[index]["transactionTime"] - items[index - 1]["transactionTime"]).days
            if days_delta > 0:
                intervals.append(days_delta)
        if len(intervals) < profile["subscription_min_occurrences"] - 1:
            continue

        cadence_info = _detect_cadence(intervals)
        if cadence_info is None:
            continue
        cadence, expected_days, tolerance_days = cadence_info

        interval_matches = sum(1 for value in intervals if abs(value - expected_days) <= tolerance_days)
        interval_ratio = interval_matches / len(intervals)
        if interval_ratio < profile["subscription_interval_match_min"]:
            continue

        amounts = [entry["amountMinor"] for entry in items]
        median_amount = int(median(amounts))
        if median_amount < profile["subscription_min_amount_minor"]:
            continue

        amount_tolerance = max(50, int(median_amount * profile["subscription_amount_variation_ratio"]))
        amount_matches = sum(1 for value in amounts if abs(value - median_amount) <= amount_tolerance)
        amount_ratio = amount_matches / len(amounts)
        if amount_ratio < profile["subscription_amount_match_min"]:
            continue

        monthly_impact = _monthly_equivalent(median_amount, cadence)
        annual_impact = monthly_impact * 12
        occurrences = len(items)
        category = Counter(entry["category"] for entry in items).most_common(1)[0][0]
        last_charge = items[-1]["transactionTime"]
        next_expected = last_charge + timedelta(days=expected_days)

        maybe_unused = (
            cadence == "monthly"
            and occurrences >= 6
            and interval_ratio >= 0.8
            and amount_ratio >= 0.85
        )
        confidence = int(round(35 + (interval_ratio * 28) + (amount_ratio * 20) + min(17, occurrences * 2)))
        if cadence == "yearly":
            confidence -= 5
        confidence = _clamp(confidence, minimum=1, maximum=99)

        title = f"Recurring {cadence} charge: {counterparty}"
        description = (
            f"{occurrences} similar payments around {_format_minor_units(median_amount)}. "
            f"Next likely charge around {next_expected.date().isoformat()}."
        )
        if maybe_unused:
            description = f"{description} Looks like an auto-renewed service; worth checking current usage."

        priority = _priority_score(
            confidence_score=confidence,
            impact_monthly_minor=monthly_impact,
            event_time=last_charge,
            reference=reference,
            profile=profile,
        )

        signals.append(
            {
                "id": f"subscription:{_slug(counterparty)}:{cadence}",
                "type": "subscription",
                "title": title,
                "description": description,
                "counterparty": counterparty,
                "category": category,
                "confidenceScore": confidence,
                "impactMonthlyMinor": monthly_impact,
                "impactAnnualMinor": annual_impact,
                "priorityScore": priority,
                "recommendation": "Review and cancel if no longer used.",
                "evidence": {
                    "cadence": cadence,
                    "occurrences": occurrences,
                    "medianAmountMinor": median_amount,
                    "intervalMatchRatio": round(interval_ratio, 3),
                    "amountMatchRatio": round(amount_ratio, 3),
                    "lastCharge": last_charge.isoformat(),
                    "nextExpectedCharge": next_expected.isoformat(),
                    "maybeUnused": maybe_unused,
                },
            }
        )

    signals.sort(key=lambda row: (-row["priorityScore"], -row["impactMonthlyMinor"], row["counterparty"]))
    return signals[:15]


def _build_trend_signals(*, transactions, window_start, reference, profile):
    trend_reference = _last_complete_month_anchor(reference)
    month_keys = _month_keys_between(window_start, trend_reference)
    if len(month_keys) < 6:
        return []
    previous_months = month_keys[-6:-3]
    recent_months = month_keys[-3:]

    category_totals = defaultdict(lambda: defaultdict(int))
    counterparty_totals = defaultdict(lambda: defaultdict(int))
    for item in transactions:
        month_key = item["transactionTime"].strftime("%Y-%m-01")
        category_totals[item["category"]][month_key] += item["amountMinor"]
        if item["counterparty"]:
            counterparty_totals[item["counterparty"]][month_key] += item["amountMinor"]

    signals = []
    signals.extend(
        _build_trend_dimension_signals(
            totals_map=category_totals,
            dimension="category",
            previous_months=previous_months,
            recent_months=recent_months,
            reference=reference,
            profile=profile,
        )
    )
    signals.extend(
        _build_trend_dimension_signals(
            totals_map=counterparty_totals,
            dimension="counterparty",
            previous_months=previous_months,
            recent_months=recent_months,
            reference=reference,
            profile=profile,
        )
    )
    signals.sort(key=lambda row: (-row["impactMonthlyMinor"], -row["confidenceScore"], row["title"]))
    return signals[:20]


def _last_complete_month_anchor(reference):
    first_day_current_month = datetime(reference.year, reference.month, 1, tzinfo=timezone.utc)
    return first_day_current_month - timedelta(days=1)


def _build_trend_dimension_signals(*, totals_map, dimension, previous_months, recent_months, reference, profile):
    signals = []
    for name, month_values in totals_map.items():
        previous_values = [month_values.get(month_key, 0) for month_key in previous_months]
        recent_values = [month_values.get(month_key, 0) for month_key in recent_months]
        previous_average = sum(previous_values) / len(previous_values)
        recent_average = sum(recent_values) / len(recent_values)
        if previous_average < profile["trend_prev_average_minor_min"]:
            continue
        delta = recent_average - previous_average
        if delta < profile["trend_delta_minor_min"]:
            continue
        ratio = delta / previous_average if previous_average else 0
        if ratio < profile["trend_delta_ratio_min"]:
            continue

        upward_steps = sum(1 for index in range(1, len(recent_values)) if recent_values[index] >= recent_values[index - 1])
        upward_ratio = upward_steps / max(1, len(recent_values) - 1)

        confidence = int(round(42 + min(28, ratio * 80) + (upward_ratio * 12) + min(12, delta / max(1, profile["trend_delta_minor_min"]) * 4)))
        confidence = _clamp(confidence, minimum=1, maximum=99)
        impact_monthly = int(round(delta))
        impact_annual = impact_monthly * 12
        priority = _priority_score(
            confidence_score=confidence,
            impact_monthly_minor=impact_monthly,
            event_time=reference,
            reference=reference,
            profile=profile,
        )

        if dimension == "category":
            title = f"Rising category spend: {name}"
            description = (
                f"3-month average increased from {_format_minor_units(previous_average)} "
                f"to {_format_minor_units(recent_average)} ({ratio * 100:.0f}% up)."
            )
            counterparty = ""
            category = name
        else:
            title = f"Rising spend with {name}"
            description = (
                f"3-month average increased from {_format_minor_units(previous_average)} "
                f"to {_format_minor_units(recent_average)} ({ratio * 100:.0f}% up)."
            )
            counterparty = name
            category = ""

        signals.append(
            {
                "id": f"trend:{dimension}:{_slug(name)}",
                "type": "trend",
                "title": title,
                "description": description,
                "counterparty": counterparty,
                "category": category,
                "confidenceScore": confidence,
                "impactMonthlyMinor": impact_monthly,
                "impactAnnualMinor": impact_annual,
                "priorityScore": priority,
                "recommendation": "Review alternatives or negotiate lower cost.",
                "evidence": {
                    "dimension": dimension,
                    "previousMonths": previous_months,
                    "recentMonths": recent_months,
                    "previousAverageMinor": int(round(previous_average)),
                    "recentAverageMinor": int(round(recent_average)),
                    "deltaMinor": impact_monthly,
                    "deltaRatio": round(ratio, 4),
                    "upwardRatio": round(upward_ratio, 3),
                },
            }
        )
    return signals


def _build_anomaly_signals(*, transactions, reference, profile):
    grouped = defaultdict(list)
    for item in transactions:
        counterparty = item["counterparty"]
        if not counterparty:
            continue
        grouped[counterparty].append(item)

    signals = []
    seen_counterparties = set()
    for counterparty, items in grouped.items():
        if len(items) < profile["anomaly_min_history"]:
            continue
        amounts = [entry["amountMinor"] for entry in items]
        median_amount = float(median(amounts))
        deviations = [abs(value - median_amount) for value in amounts]
        mad = float(median(deviations))
        threshold = median_amount + max(profile["anomaly_min_lift_minor"], (mad * profile["anomaly_mad_multiplier"]))

        candidate = None
        for entry in items:
            amount_minor = entry["amountMinor"]
            if amount_minor < profile["anomaly_min_amount_minor"]:
                continue
            if amount_minor <= threshold:
                continue
            if candidate is None or amount_minor > candidate["amountMinor"]:
                candidate = entry
        if candidate is None:
            continue

        seen_counterparties.add(counterparty)
        excess_minor = int(candidate["amountMinor"] - median_amount)
        confidence = int(round(45 + min(30, (excess_minor / max(1, median_amount)) * 40) + min(15, len(items) * 2)))
        confidence = _clamp(confidence, minimum=1, maximum=99)

        impact_annual = max(0, excess_minor)
        impact_monthly = int(round(impact_annual / 12))
        priority = _priority_score(
            confidence_score=confidence,
            impact_monthly_minor=impact_monthly,
            event_time=candidate["transactionTime"],
            reference=reference,
            profile=profile,
        )
        signals.append(
            {
                "id": f"anomaly:{_slug(counterparty)}:{candidate['feedItemUid']}",
                "type": "anomaly",
                "title": f"Unusual one-off spend: {counterparty}",
                "description": (
                    f"Charge of {_format_minor_units(candidate['amountMinor'])} vs typical "
                    f"{_format_minor_units(median_amount)}."
                ),
                "counterparty": counterparty,
                "category": candidate["category"],
                "confidenceScore": confidence,
                "impactMonthlyMinor": impact_monthly,
                "impactAnnualMinor": impact_annual,
                "priorityScore": priority,
                "recommendation": "Check if this was expected or avoidable.",
                "evidence": {
                    "transactionId": candidate["feedItemUid"],
                    "transactionTime": candidate["transactionTime"].isoformat(),
                    "amountMinor": candidate["amountMinor"],
                    "medianAmountMinor": int(round(median_amount)),
                    "mad": int(round(mad)),
                    "thresholdMinor": int(round(threshold)),
                    "historyCount": len(items),
                },
            }
        )

    all_amounts = sorted(entry["amountMinor"] for entry in transactions if entry["amountMinor"] > 0)
    if all_amounts:
        percentile_90 = _percentile(all_amounts, 0.9)
    else:
        percentile_90 = 0
    singleton_floor = max(percentile_90, profile["anomaly_singleton_min_amount_minor"])

    for counterparty, items in grouped.items():
        if counterparty in seen_counterparties:
            continue
        if len(items) != 1:
            continue
        item = items[0]
        amount_minor = item["amountMinor"]
        if amount_minor < singleton_floor:
            continue

        confidence = _clamp(int(round(52 + min(18, (amount_minor / max(1, singleton_floor)) * 15))), minimum=1, maximum=99)
        impact_annual = amount_minor
        impact_monthly = int(round(amount_minor / 12))
        priority = _priority_score(
            confidence_score=confidence,
            impact_monthly_minor=impact_monthly,
            event_time=item["transactionTime"],
            reference=reference,
            profile=profile,
        )
        signals.append(
            {
                "id": f"anomaly:new:{_slug(counterparty)}:{item['feedItemUid']}",
                "type": "anomaly",
                "title": f"High first-time spend: {counterparty}",
                "description": f"Single charge of {_format_minor_units(amount_minor)} at a new/rare counterparty.",
                "counterparty": counterparty,
                "category": item["category"],
                "confidenceScore": confidence,
                "impactMonthlyMinor": impact_monthly,
                "impactAnnualMinor": impact_annual,
                "priorityScore": priority,
                "recommendation": "Review merchant and set spending guardrails if needed.",
                "evidence": {
                    "transactionId": item["feedItemUid"],
                    "transactionTime": item["transactionTime"].isoformat(),
                    "amountMinor": amount_minor,
                    "percentile90Minor": percentile_90,
                },
            }
        )

    signals.sort(key=lambda row: (-row["priorityScore"], -row["confidenceScore"], row["title"]))
    return signals[:20]


def _detect_cadence(intervals):
    median_interval = median(intervals)
    if 5 <= median_interval <= 9:
        return "weekly", 7, 2
    if 24 <= median_interval <= 35:
        return "monthly", 30, 5
    if 80 <= median_interval <= 100:
        return "quarterly", 91, 12
    if 330 <= median_interval <= 395:
        return "yearly", 365, 25
    return None


def _monthly_equivalent(amount_minor, cadence):
    if cadence == "weekly":
        return int(round((amount_minor * 52) / 12))
    if cadence == "quarterly":
        return int(round(amount_minor / 3))
    if cadence == "yearly":
        return int(round(amount_minor / 12))
    return int(amount_minor)


def _priority_score(*, confidence_score, impact_monthly_minor, event_time, reference, profile):
    impact_scale = max(1, profile["impact_scale_minor"])
    impact_component = min(100.0, (impact_monthly_minor / impact_scale) * 100.0)
    age_days = max(0, (reference - event_time).days)
    recency_component = max(0.0, 100.0 - (age_days * 1.5))
    value = (confidence_score * 0.55) + (impact_component * 0.35) + (recency_component * 0.10)
    return _clamp(int(round(value)), minimum=1, maximum=100)


def _month_keys_between(start_time, reference_time):
    current = datetime(start_time.year, start_time.month, 1, tzinfo=timezone.utc)
    end_month = datetime(reference_time.year, reference_time.month, 1, tzinfo=timezone.utc)
    keys = []
    while current <= end_month:
        keys.append(current.strftime("%Y-%m-01"))
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            current = datetime(current.year, current.month + 1, 1, tzinfo=timezone.utc)
    return keys


def _percentile(values, percentile_rank):
    if not values:
        return 0
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * percentile_rank
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return values[lower]
    lower_value = values[lower]
    upper_value = values[upper]
    return int(round(lower_value + ((rank - lower) * (upper_value - lower_value))))


def _slug(value):
    cleaned = re.sub(r"[^a-z0-9]+", "-", (value or "").lower()).strip("-")
    if cleaned:
        return cleaned
    return "unknown"


def _format_minor_units(amount_minor):
    amount = int(round(amount_minor))
    return f"£{amount / 100:.2f}"


def _clamp(value, *, minimum, maximum):
    return max(minimum, min(maximum, value))


def _normalize_to_utc(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
