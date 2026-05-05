import math
import re
from collections import Counter
from datetime import datetime, timedelta, timezone

from django.db.models.functions import Upper

from starling_web.spaces.models import FeedItem, HolidayMerchantOverride, HolidaySuggestionDecision

from .analytics import EXCLUDED_TRANSFER_SOURCES


CONFIDENCE_PROFILES = {
    "high": {
        "candidate_score_min": 18,
        "suggested_score_min": 26,
        "cluster_score_min": 75,
        "rare_merchant_max_count": 1,
        "frequent_merchant_min_count": 4,
        "domestic_nearby_score_min": 20,
        "domestic_grocery_score_min": 22,
    },
    "balanced": {
        "candidate_score_min": 14,
        "suggested_score_min": 22,
        "cluster_score_min": 58,
        "rare_merchant_max_count": 2,
        "frequent_merchant_min_count": 4,
        "domestic_nearby_score_min": 18,
        "domestic_grocery_score_min": 20,
    },
    "broad": {
        "candidate_score_min": 10,
        "suggested_score_min": 18,
        "cluster_score_min": 44,
        "rare_merchant_max_count": 3,
        "frequent_merchant_min_count": 5,
        "domestic_nearby_score_min": 16,
        "domestic_grocery_score_min": 18,
    },
}

SCOPES = {"all", "foreign", "domestic"}
TRIP_CATEGORIES = {"Eating Out", "Groceries", "Transport", "Holidays"}
ACCOMMODATION_PATTERN = re.compile(
    r"(airbnb|vrbo|hotel|hilton|inn\b|resort|accommodat|booking\.com|speedybooker|q hotels)",
    re.IGNORECASE,
)
TRAVEL_PATTERN = re.compile(
    r"(easyjet|trainline|tfl\b|uber\b|taxi|stagecoach|ringgo|paybyphone|mipermit|justpark|citipark|parking|rail|airport)",
    re.IGNORECASE,
)
CASH_ACCESS_PATTERN = re.compile(r"(cash machine|cash withdrawal|\batm\b)", re.IGNORECASE)
HOME_PLACE_PATTERN = re.compile(r"(stroud|oxford)", re.IGNORECASE)


def calculate_holiday_signals(
    *,
    days: int,
    reference_time=None,
    start_time=None,
    confidence_mode="balanced",
    scope="all",
    include_reviewed=False,
):
    if days <= 0:
        raise ValueError("days must be positive")
    if confidence_mode not in CONFIDENCE_PROFILES:
        raise ValueError(f"Unsupported confidence mode: {confidence_mode}")
    if scope not in SCOPES:
        raise ValueError(f"Unsupported scope: {scope}")

    reference = _normalize_to_utc(reference_time or datetime.now(timezone.utc))
    window_start = reference - timedelta(days=days)
    if start_time is not None:
        window_start = _normalize_to_utc(start_time)
    window_end = reference + timedelta(seconds=1)

    profile = CONFIDENCE_PROFILES[confidence_mode]
    transactions = _load_spending_transactions(window_start=window_start, window_end=window_end)
    merchant_counts = Counter(item["merchantKey"] for item in transactions if item["merchantKey"])
    history_counts = _load_merchant_history_counts(
        window_start=max(window_start, window_end - timedelta(days=365)),
        window_end=window_end,
    )
    merchant_overrides = _load_holiday_merchant_overrides()
    review_decisions = _load_holiday_review_decisions()

    scored = []
    for item in transactions:
        merchant_override_type = merchant_overrides.get(item["merchantKey"])
        if merchant_override_type == "ignore":
            continue
        review_decision = review_decisions.get(item["feedItemUid"])
        if review_decision == "accepted" and not include_reviewed:
            continue
        enriched = dict(item)
        score, reason_codes, reason_labels = _score_transaction(
            item,
            merchant_counts=merchant_counts,
            history_counts=history_counts,
            profile=profile,
            merchant_override_type=merchant_override_type,
            review_decision=review_decision,
        )
        enriched["score"] = score
        enriched["reasonCodes"] = reason_codes
        enriched["reasons"] = reason_labels
        enriched["merchantOverrideType"] = merchant_override_type
        enriched["reviewDecision"] = review_decision
        enriched["isCandidate"] = _is_candidate(enriched, profile)
        scored.append(enriched)

    seed_windows = _build_seed_windows(scored)
    clusters = []
    for index, window in enumerate(seed_windows, start=1):
        cluster = _build_cluster(window, scored, profile=profile, cluster_index=index)
        if cluster is None:
            continue
        if scope == "foreign" and not cluster["isForeign"]:
            continue
        if scope == "domestic" and cluster["isForeign"]:
            continue
        clusters.append(cluster)

    clusters.sort(
        key=lambda item: (
            -item["confidenceScore"],
            -item["totalSuggestedMinor"],
            item["start"],
        )
    )

    return {
        "summary": {
            "clustersCount": len(clusters),
            "transactionsCount": sum(cluster["transactionCount"] for cluster in clusters),
            "suggestedTransactionsCount": sum(cluster["suggestedTransactionCount"] for cluster in clusters),
            "totalSuggestedMinor": sum(cluster["totalSuggestedMinor"] for cluster in clusters),
        },
        "clusters": clusters,
        "window": {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
            "days": days,
        },
        "confidenceMode": confidence_mode,
        "scope": scope,
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
            "raw_json",
        )
        .order_by("transaction_time", "feed_item_uid")
    )

    transactions = []
    for row in rows:
        raw = row["raw_json"] or {}
        reference = (raw.get("reference") or "").strip()
        source_amount = raw.get("sourceAmount") or {}
        source_currency = (source_amount.get("currency") or "").strip()
        country = (raw.get("country") or "").strip().upper()
        counterparty = (row["counterparty"] or "").strip()
        category = (row["classified_category"] or "").strip() or "Uncategorised"
        merchant_key = _merchant_key(counterparty or reference)
        transactions.append(
            {
                "feedItemUid": row["feed_item_uid"],
                "transactionTime": _normalize_to_utc(row["transaction_time"]),
                "counterparty": counterparty,
                "category": category,
                "amountMinorUnits": int(-(row["amount_minor_units"] or 0)),
                "currency": (row["currency"] or "GBP").strip() or "GBP",
                "source": (row["source"] or "").strip(),
                "country": country,
                "reference": reference,
                "sourceCurrency": source_currency,
                "merchantKey": merchant_key,
            }
        )
    return transactions


def _load_merchant_history_counts(*, window_start, window_end):
    rows = (
        FeedItem.objects.filter(
            transaction_time__gte=window_start,
            transaction_time__lt=window_end,
            amount_minor_units__lt=0,
        )
        .annotate(source_upper=Upper("source"))
        .exclude(source_upper__in=EXCLUDED_TRANSFER_SOURCES)
        .values("counterparty", "raw_json")
    )

    counts = Counter()
    for row in rows:
        raw = row["raw_json"] or {}
        reference = (raw.get("reference") or "").strip()
        counterparty = (row["counterparty"] or "").strip()
        merchant_key = _merchant_key(counterparty or reference)
        if merchant_key:
            counts[merchant_key] += 1
    return counts


def _load_holiday_merchant_overrides():
    return dict(
        HolidayMerchantOverride.objects.values_list("merchant_key", "override_type")
    )


def _load_holiday_review_decisions():
    return dict(
        HolidaySuggestionDecision.objects.values_list("feed_item_uid", "decision")
    )


def _score_transaction(item, *, merchant_counts, history_counts, profile, merchant_override_type, review_decision):
    score = 0
    reason_codes = []
    reason_labels = []
    counterparty = item["counterparty"]
    reference = item["reference"]
    category = item["category"]
    country = item["country"]
    source_currency = item["sourceCurrency"]
    merchant_key = item["merchantKey"]
    is_home_place = _matches(HOME_PLACE_PATTERN, counterparty, reference)

    if country and country != "GB":
        score += 55
        reason_codes.append("foreign_spend")
        reason_labels.append(f"Foreign spend ({country})")
    if source_currency and source_currency != "GBP":
        score += 18
        reason_codes.append("foreign_currency")
        reason_labels.append(f"Foreign currency ({source_currency})")
    if _matches(ACCOMMODATION_PATTERN, counterparty, reference) and not is_home_place:
        score += 40
        reason_codes.append("accommodation_anchor")
        reason_labels.append("Accommodation anchor")
    if _matches(TRAVEL_PATTERN, counterparty, reference):
        score += 18
        reason_codes.append("travel_anchor")
        reason_labels.append("Travel anchor")
    if category == "Holidays":
        score += 20
        reason_codes.append("holiday_category")
        reason_labels.append("Already categorised as Holidays")
    elif category in TRIP_CATEGORIES:
        score += 10
        reason_codes.append("trip_category")
        reason_labels.append(f"Trip-like category: {category}")

    merchant_count = merchant_counts.get(merchant_key, 0)
    if merchant_key and merchant_count <= profile["rare_merchant_max_count"]:
        score += 12
        reason_codes.append("rare_merchant")
        reason_labels.append("Rare merchant in current window")

    historical_merchant_count = history_counts.get(merchant_key, 0)
    if merchant_key and historical_merchant_count >= profile["frequent_merchant_min_count"]:
        score -= 14
        reason_codes.append("frequent_merchant")
        reason_labels.append("Frequent merchant in recent history")

    if merchant_override_type == "home":
        score -= 30
        reason_codes.append("home_override")
        reason_labels.append("Merchant marked as home")
    elif merchant_override_type == "holiday_anchor":
        score += 40
        reason_codes.append("holiday_anchor_override")
        reason_labels.append("Merchant marked as holiday anchor")

    if review_decision == "accepted":
        score += 30
        reason_codes.append("accepted_review")
        reason_labels.append("Previously accepted")
    elif review_decision == "rejected":
        score -= 30
        reason_codes.append("rejected_review")
        reason_labels.append("Previously rejected")

    if is_home_place:
        score -= 18
        reason_codes.append("home_place")
        reason_labels.append("Home-place match")

    return score, reason_codes, reason_labels


def _is_candidate(item, profile):
    if item.get("reviewDecision") == "rejected":
        return False
    if item["country"] and item["country"] != "GB":
        return True
    if "accommodation_anchor" in item["reasonCodes"]:
        return True
    if "holiday_category" in item["reasonCodes"]:
        return True
    if "holiday_anchor_override" in item["reasonCodes"]:
        return True
    if "travel_anchor" in item["reasonCodes"]:
        return True
    return item["score"] >= profile["candidate_score_min"]


def _build_seed_windows(transactions):
    windows = []
    for item in transactions:
        if item.get("reviewDecision") == "rejected":
            continue
        tx_day = item["transactionTime"].date()
        reason_codes = set(item["reasonCodes"])
        if "foreign_spend" in reason_codes or "foreign_currency" in reason_codes:
            windows.append(
                {
                    "start": tx_day - timedelta(days=1),
                    "end": tx_day + timedelta(days=1),
                    "reasonCodes": {"foreign_spend"},
                }
            )
        elif (
            "accommodation_anchor" in reason_codes
            or "holiday_category" in reason_codes
            or "holiday_anchor_override" in reason_codes
        ):
            windows.append(
                {
                    "start": tx_day - timedelta(days=1),
                    "end": tx_day + timedelta(days=3),
                    "reasonCodes": (
                        {"accommodation_anchor"} & reason_codes
                        or {"holiday_category"} & reason_codes
                        or {"holiday_anchor_override"} & reason_codes
                    ),
                }
            )
        elif (
            "travel_anchor" in reason_codes
            and item["category"] in TRIP_CATEGORIES
            and "home_place" not in reason_codes
            and "frequent_merchant" not in reason_codes
        ):
            windows.append(
                {
                    "start": tx_day,
                    "end": tx_day + timedelta(days=1),
                    "reasonCodes": {"travel_anchor"},
                }
            )

    day_map = {}
    for item in transactions:
        if item.get("reviewDecision") == "rejected":
            continue
        if item["category"] not in TRIP_CATEGORIES:
            continue
        if "home_place" in item["reasonCodes"] or "home_override" in item["reasonCodes"]:
            continue
        if "rare_merchant" not in item["reasonCodes"]:
            continue
        tx_day = item["transactionTime"].date()
        stats = day_map.setdefault(
            tx_day,
            {
                "merchantKeys": set(),
                "categories": set(),
                "count": 0,
            },
        )
        stats["count"] += 1
        if item["merchantKey"]:
            stats["merchantKeys"].add(item["merchantKey"])
        stats["categories"].add(item["category"])

    for tx_day, stats in day_map.items():
        if stats["count"] >= 3 and len(stats["merchantKeys"]) >= 3 and len(stats["categories"]) >= 2:
            windows.append(
                {
                    "start": tx_day,
                    "end": tx_day + timedelta(days=2),
                    "reasonCodes": {"dense_domestic_burst"},
                }
            )

    if not windows:
        return []

    windows.sort(key=lambda item: (item["start"], item["end"]))
    merged = [windows[0]]
    for window in windows[1:]:
        current = merged[-1]
        if window["start"] <= current["end"]:
            current["end"] = max(current["end"], window["end"])
            current["reasonCodes"].update(window["reasonCodes"])
        else:
            merged.append(window)
    return merged


def _build_cluster(window, transactions, *, profile, cluster_index):
    reason_codes = set(window["reasonCodes"])
    window_start = window["start"]
    window_end = window["end"]
    cluster_context = _build_cluster_context(window, transactions)
    cluster_transactions = []
    for item in transactions:
        tx_day = item["transactionTime"].date()
        if tx_day < window_start or tx_day > window_end:
            continue
        if not _cluster_supports_transaction(item, cluster_context=cluster_context, profile=profile):
            continue
        tx = dict(item)
        tx["suggested"] = _transaction_is_suggested(tx, cluster_context=cluster_context, profile=profile)
        cluster_transactions.append(tx)

    if not cluster_transactions:
        return None

    reason_labels = []
    for item in cluster_transactions:
        reason_codes.update(item["reasonCodes"])
        for label in item["reasons"]:
            if label not in reason_labels:
                reason_labels.append(label)

    distinct_days = sorted({item["transactionTime"].date() for item in cluster_transactions})
    distinct_categories = {item["category"] for item in cluster_transactions if item["category"] in TRIP_CATEGORIES}
    distinct_rare_merchants = {
        item["merchantKey"]
        for item in cluster_transactions
        if "rare_merchant" in item["reasonCodes"] and item["merchantKey"]
    }
    if len(distinct_days) >= 2:
        reason_codes.add("multi_day_cluster")
    if len(distinct_categories) >= 2:
        reason_codes.add("mixed_trip_categories")
    if len(distinct_rare_merchants) >= 3:
        reason_codes.add("unusual_merchant_burst")
    is_foreign = "foreign_spend" in reason_codes or "foreign_currency" in reason_codes

    cluster_score = sum(max(item["score"], 0) for item in cluster_transactions)
    if "foreign_spend" in reason_codes:
        cluster_score += 18
    if (
        "accommodation_anchor" in reason_codes
        or "holiday_category" in reason_codes
        or "holiday_anchor_override" in reason_codes
    ):
        cluster_score += 18
    if "dense_domestic_burst" in reason_codes:
        cluster_score += 12
    if len(distinct_categories) >= 2:
        cluster_score += 8
    if len(distinct_days) >= 2:
        cluster_score += 8

    if cluster_score < profile["cluster_score_min"]:
        return None
    strong_anchor_reasons = {
        "foreign_spend",
        "accommodation_anchor",
        "holiday_category",
        "holiday_anchor_override",
        "travel_anchor",
    }
    if "dense_domestic_burst" in reason_codes and len(distinct_days) < 2 and not (strong_anchor_reasons & reason_codes):
        return None

    confidence_score = min(99, max(1, int(math.ceil(cluster_score))))
    return {
        "id": f"holiday-cluster-{cluster_index}",
        "title": f"Holiday-like spend: {window_start.isoformat()} to {window_end.isoformat()}",
        "start": min(item["transactionTime"] for item in cluster_transactions).isoformat(),
        "end": max(item["transactionTime"] for item in cluster_transactions).isoformat(),
        "reasonCodes": sorted(reason_codes),
        "reasons": reason_labels,
        "confidenceScore": confidence_score,
        "confidenceBand": _confidence_band(confidence_score),
        "isForeign": is_foreign,
        "transactionCount": len(cluster_transactions),
        "suggestedTransactionCount": sum(1 for item in cluster_transactions if item["suggested"]),
        "totalSuggestedMinor": sum(item["amountMinorUnits"] for item in cluster_transactions if item["suggested"]),
        "transactions": [
            {
                "feedItemUid": item["feedItemUid"],
                "transactionTime": item["transactionTime"].isoformat(),
                "counterparty": item["counterparty"],
                "category": item["category"],
                "amountMinorUnits": item["amountMinorUnits"],
                "currency": item["currency"],
                "source": item["source"],
                "country": item["country"],
                "reference": item["reference"],
                "merchantKey": item["merchantKey"],
                "score": item["score"],
                "reasonCodes": item["reasonCodes"],
                "reasons": item["reasons"],
                "merchantOverrideType": item["merchantOverrideType"],
                "reviewDecision": item["reviewDecision"],
                "suggested": item["suggested"],
            }
            for item in sorted(cluster_transactions, key=lambda row: row["transactionTime"])
        ],
    }


def _build_cluster_context(window, transactions):
    window_start = window["start"]
    window_end = window["end"]
    anchor_days = []
    foreign_anchor_days = []
    accommodation_anchor_days = []
    travel_anchor_days = []
    for item in transactions:
        tx_day = item["transactionTime"].date()
        if tx_day < window_start or tx_day > window_end:
            continue
        if not _is_anchor_transaction(item):
            continue
        anchor_days.append(tx_day)
        if item["country"] and item["country"] != "GB":
            foreign_anchor_days.append(tx_day)
        if (
            "accommodation_anchor" in item["reasonCodes"]
            or "holiday_category" in item["reasonCodes"]
            or "holiday_anchor_override" in item["reasonCodes"]
        ):
            accommodation_anchor_days.append(tx_day)
        if "travel_anchor" in item["reasonCodes"]:
            travel_anchor_days.append(tx_day)
    return {
        "seedReasonCodes": set(window["reasonCodes"]),
        "anchorDays": sorted(set(anchor_days)),
        "foreignAnchorDays": sorted(set(foreign_anchor_days)),
        "accommodationAnchorDays": sorted(set(accommodation_anchor_days)),
        "travelAnchorDays": sorted(set(travel_anchor_days)),
        "isForeign": "foreign_spend" in window["reasonCodes"],
    }


def _cluster_supports_transaction(item, *, cluster_context, profile):
    if cluster_context["isForeign"]:
        return _supports_foreign_cluster_transaction(item, cluster_context=cluster_context)
    if _is_anchor_transaction(item):
        return True
    if item["category"] not in TRIP_CATEGORIES:
        return False
    if (
        "home_place" in item["reasonCodes"]
        or "home_override" in item["reasonCodes"]
        or "frequent_merchant" in item["reasonCodes"]
    ):
        return False

    tx_day = item["transactionTime"].date()
    nearest_anchor_distance = _nearest_anchor_distance(tx_day, cluster_context["anchorDays"])
    if nearest_anchor_distance is None:
        if "dense_domestic_burst" not in cluster_context["seedReasonCodes"]:
            return False
        return "rare_merchant" in item["reasonCodes"] and item["score"] >= 10

    nearest_accommodation_distance = _nearest_anchor_distance(tx_day, cluster_context["accommodationAnchorDays"])
    nearest_travel_distance = _nearest_anchor_distance(tx_day, cluster_context["travelAnchorDays"])
    is_near_accommodation_anchor = nearest_accommodation_distance is not None and nearest_accommodation_distance <= 3
    is_near_travel_anchor = nearest_travel_distance is not None and nearest_travel_distance <= 1
    if not is_near_accommodation_anchor and not is_near_travel_anchor:
        return False
    if item["reviewDecision"] == "rejected":
        return item["category"] in TRIP_CATEGORIES
    if "rare_merchant" in item["reasonCodes"] and item["score"] >= 10:
        return True
    if item["category"] == "Groceries":
        return item["score"] >= profile["domestic_grocery_score_min"]
    if item["category"] in {"Eating Out", "Transport", "Holidays"}:
        return item["score"] >= profile["domestic_nearby_score_min"]
    return False


def _transaction_is_suggested(item, *, cluster_context, profile):
    if item["reviewDecision"] == "accepted":
        return False
    if item["reviewDecision"] == "rejected":
        return False
    if not _cluster_supports_transaction(item, cluster_context=cluster_context, profile=profile):
        return False
    if cluster_context["isForeign"]:
        return True
    if _is_anchor_transaction(item):
        return True
    if item["score"] >= profile["suggested_score_min"]:
        return True
    if item["category"] == "Groceries":
        return item["score"] >= profile["domestic_grocery_score_min"]
    return item["score"] >= profile["domestic_nearby_score_min"]


def _is_anchor_transaction(item):
    if item.get("reviewDecision") == "rejected":
        return False
    if item.get("merchantOverrideType") == "home":
        return False
    return bool(
        _is_foreign_trip_supporting_transaction(item)
        or "accommodation_anchor" in item["reasonCodes"]
        or "holiday_category" in item["reasonCodes"]
        or "holiday_anchor_override" in item["reasonCodes"]
        or "travel_anchor" in item["reasonCodes"]
    )


def _supports_foreign_cluster_transaction(item, *, cluster_context):
    nearest_foreign_distance = _nearest_anchor_distance(
        item["transactionTime"].date(),
        cluster_context["foreignAnchorDays"],
    )
    if nearest_foreign_distance is None:
        return False
    if item["reviewDecision"] == "rejected":
        return item["category"] in TRIP_CATEGORIES and nearest_foreign_distance <= 1
    if _is_foreign_trip_supporting_transaction(item):
        if item["category"] == "Groceries":
            return "rare_merchant" in item["reasonCodes"] or nearest_foreign_distance <= 1
        return nearest_foreign_distance <= 1 and item["score"] >= 10
    if (
        "home_place" in item["reasonCodes"]
        or "home_override" in item["reasonCodes"]
        or "frequent_merchant" in item["reasonCodes"]
    ):
        return False
    if (
        "accommodation_anchor" in item["reasonCodes"]
        or "holiday_category" in item["reasonCodes"]
        or "holiday_anchor_override" in item["reasonCodes"]
    ):
        return nearest_foreign_distance <= 2
    if "travel_anchor" in item["reasonCodes"]:
        return nearest_foreign_distance <= 1
    return False


def _is_foreign_trip_supporting_transaction(item):
    if not (item["country"] and item["country"] != "GB"):
        return False
    if item["category"] in TRIP_CATEGORIES:
        return True
    return _matches(CASH_ACCESS_PATTERN, item["counterparty"], item["reference"])


def _nearest_anchor_distance(tx_day, anchor_days):
    if not anchor_days:
        return None
    return min(abs((tx_day - anchor_day).days) for anchor_day in anchor_days)


def _merchant_key(value):
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _matches(pattern, counterparty, reference):
    return bool(pattern.search(counterparty or "") or pattern.search(reference or ""))


def _normalize_to_utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _confidence_band(score):
    if score >= 80:
        return "high"
    if score >= 55:
        return "medium"
    return "low"
