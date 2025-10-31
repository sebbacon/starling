import json
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from django import forms
from django.conf import settings
from django.db.models import Q, Max
from django.db.models.functions import Upper
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.http import require_GET, require_POST, require_http_methods

from starling_spaces.analytics import (
    calculate_spend_by_category,
    EXCLUDED_TRANSFER_SOURCES,
)
from starling_web.spaces.models import Category, ClassificationRule, FeedItem


RULE_TYPE_CHOICES = [
    ("counterparty_regex", "Counterparty matches regular expression"),
    ("space_name_regex", "Space name matches regular expression"),
    ("source_regex", "Transaction source matches regular expression"),
    ("space", "Specific space UID"),
    ("raw_path", "Value at JSON path matches"),
    ("starling_category", "Starling spending category fallback"),
    ("space_name", "Use space name as category"),
]


class ClassificationRuleForm(forms.ModelForm):
    rule_type = forms.ChoiceField(choices=RULE_TYPE_CHOICES, label="Rule type")

    class Meta:
        model = ClassificationRule
        fields = [
            "position",
            "rule_type",
            "category",
            "reason",
            "pattern",
            "space_uid",
            "json_path",
            "start_date",
            "end_date",
        ]
        widgets = {
            "reason": forms.TextInput(attrs={"placeholder": "Short label describing why this rule exists"}),
            "pattern": forms.TextInput(attrs={"placeholder": "e.g. (?i)co-op"}),
            "start_date": forms.DateInput(attrs={"type": "date"}),
            "end_date": forms.DateInput(attrs={"type": "date"}),
        }

    def __init__(self, *args, category_choices=None, **kwargs):
        self.category_choices = category_choices or []
        super().__init__(*args, **kwargs)
        choices = [("", "Select a category")]
        for name in sorted(set(self.category_choices)):
            choices.append((name, name))
        current = None
        if self.is_bound:
            current = self.data.get("category")
        else:
            current = self.initial.get("category") or getattr(self.instance, "category", None)
        if current and current not in [value for value, _ in choices]:
            choices.append((current, current))
        self.fields["category"] = forms.ChoiceField(
            choices=choices,
            required=False,
            label="Category",
        )
        self.fields["category"].initial = current or ""

    def clean(self):
        cleaned = super().clean()
        start = cleaned.get("start_date")
        end = cleaned.get("end_date")
        if start and end and start > end:
            raise forms.ValidationError("End date must be on or after the start date.")

        rule_type = cleaned.get("rule_type")
        category = cleaned.get("category")
        pattern = cleaned.get("pattern")
        space_uid = cleaned.get("space_uid")
        json_path = cleaned.get("json_path")

        if category == "":
            category = None
            cleaned["category"] = None

        if rule_type in {"counterparty_regex", "space_name_regex", "source_regex"}:
            if not pattern:
                self.add_error("pattern", "Provide a regular expression to match against.")
            if not category:
                self.add_error("category", "Select the category to apply when the pattern matches.")

        if rule_type == "space":
            if not space_uid:
                self.add_error("space_uid", "Provide the Starling space UID to match.")
            if not category:
                self.add_error("category", "Select the category to apply when the space matches.")

        if rule_type == "raw_path":
            if not json_path:
                self.add_error("json_path", "Provide the dot-separated JSON path to inspect.")
            if not category:
                self.add_error("category", "Select the category to apply when the JSON path has a value.")

        if rule_type == "starling_category" and category:
            # allow explicit override but ensure not empty string
            cleaned["category"] = category.strip() or None

        return cleaned


@require_GET
def spending(request, category_name=None, counterparty_name=None):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        _, _, days, _ = _resolve_time_window(request, default_days)
    except ValueError:
        days = default_days

    search_query = (request.GET.get("search") or "").strip()

    category_options_query = (
        Category.objects.filter(category_type="spending")
        .exclude(name__isnull=True)
        .exclude(name="")
        .order_by("name")
        .values_list("name", flat=True)
        .distinct()
    )
    category_options = list(category_options_query)
    if "Uncategorised" not in category_options:
        category_options.append("Uncategorised")

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
            "category_options": category_options,
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


@require_POST
def recategorise_transactions(request):
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (TypeError, json.JSONDecodeError):
        return JsonResponse({"error": "invalid json payload"}, status=400)

    feed_ids = payload.get("feedItemUids")
    category = payload.get("category")
    if not isinstance(feed_ids, list) or not feed_ids:
        return JsonResponse({"error": "feedItemUids is required"}, status=400)
    if not isinstance(category, str) or not category.strip():
        return JsonResponse({"error": "category is required"}, status=400)

    cleaned_category = category.strip()
    valid_ids = {uid.strip() for uid in feed_ids if isinstance(uid, str) and uid.strip()}
    if not valid_ids:
        return JsonResponse({"error": "no valid feedItemUids provided"}, status=400)

    updated = FeedItem.objects.filter(feed_item_uid__in=valid_ids).update(
        classified_category=cleaned_category,
        classification_reason="manual",
    )

    return JsonResponse({"updated": updated, "category": cleaned_category})


@require_http_methods(["GET", "POST"])
def manage_classification_rules(request):
    rules = ClassificationRule.objects.order_by("position", "id")
    category_options_query = (
        Category.objects.filter(category_type="spending")
        .exclude(name__isnull=True)
        .exclude(name="")
        .values_list("name", flat=True)
        .distinct()
    )
    category_options = list(category_options_query)
    if "Uncategorised" not in category_options:
        category_options.append("Uncategorised")
    selected_rule = None
    selected_rule_id = request.GET.get("rule")
    if selected_rule_id:
        selected_rule = ClassificationRule.objects.filter(pk=selected_rule_id).first()

    status = request.GET.get("status")
    message = {
        "created": "Classification rule created successfully.",
        "updated": "Classification rule updated successfully.",
        "deleted": "Classification rule deleted.",
    }.get(status or "")

    if request.method == "POST":
        action = request.POST.get("action")
        rule_id = request.POST.get("rule_id")

        if action == "delete" and rule_id:
            rule = get_object_or_404(ClassificationRule, pk=rule_id)
            rule.delete()
            return redirect(f"{reverse('spaces:classification-rules')}?status=deleted")

        instance = None
        if rule_id:
            instance = get_object_or_404(ClassificationRule, pk=rule_id)
            selected_rule = instance

        form = ClassificationRuleForm(request.POST, instance=instance, category_choices=category_options)
        if form.is_valid():
            rule = form.save(commit=False)
            if rule.position is None:
                max_position = ClassificationRule.objects.aggregate(Max("position"))["position__max"] or -1
                rule.position = max_position + 1
            rule.save()
            next_status = "updated" if instance else "created"
            return redirect(f"{reverse('spaces:classification-rules')}?status={next_status}&rule={rule.pk}")
    else:
        initial = {}
        max_position = ClassificationRule.objects.aggregate(Max("position"))["position__max"] or -1
        initial["position"] = max_position + 1
        form = ClassificationRuleForm(initial=initial, category_choices=category_options)
        if selected_rule:
            form = ClassificationRuleForm(instance=selected_rule, category_choices=category_options)

    context = {
        "rules": rules,
        "form": form,
        "selected_rule": selected_rule,
        "message": message,
        "rule_type_guidance": RULE_TYPE_CHOICES,
        "category_choices": sorted(set(category_options)),
    }
    return render(request, "spaces/classification_rules.html", context)


@require_GET
def space_lookup(request):
    term = (request.GET.get("q") or "").strip()
    if not term:
        return JsonResponse({"results": []})

    matches = (
        Category.objects.filter(category_type="space", name__icontains=term)
        .order_by("name")
        .values("space_uid", "name")[:10]
    )
    results = [
        {
            "spaceUid": item["space_uid"],
            "name": item["name"] or item["space_uid"],
        }
        for item in matches
    ]
    return JsonResponse({"results": results})


@require_GET
def json_path_lookup(request):
    term = (request.GET.get("q") or "").strip()

    collected = []
    seen = set()

    def iter_paths(data, prefix=None):
        if isinstance(data, dict):
            for key, value in data.items():
                next_prefix = f"{prefix}.{key}" if prefix else key
                yield from iter_paths(value, next_prefix)
        elif isinstance(data, list):
            for index, value in enumerate(data[:5]):
                next_prefix = f"{prefix}[{index}]" if prefix else f"[{index}]"
                yield from iter_paths(value, next_prefix)
        else:
            if prefix:
                yield prefix

    queryset = FeedItem.objects.exclude(raw_json={}).order_by("-transaction_time")[:200]
    for item in queryset:
        for path in iter_paths(item.raw_json):
            if term:
                if term.lower() not in path.lower():
                    continue
            if path not in seen:
                seen.add(path)
                collected.append(path)
            if len(collected) >= 10:
                break
        if len(collected) >= 10:
            break

    return JsonResponse({"results": collected})


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
