import json
import math
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from django import forms
from django.conf import settings
from django.db.models import Q, Max
from django.db.models.functions import Upper
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.http import require_GET, require_POST, require_http_methods
from django.core.management import call_command

from starling_spaces.analytics import (
    calculate_income_by_category,
    calculate_monthly_cashflow_totals,
    calculate_spend_by_category,
    summarise_category_totals,
    EXCLUDED_TRANSFER_SOURCES,
)
from starling_spaces.savings import calculate_savings_signals
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
RULE_TYPE_LABELS = dict(RULE_TYPE_CHOICES)

CATEGORY_PERIODS = {
    "past_month": {"label": "Past 30 days", "metric": "total"},
    "all_time": {"label": "All time", "metric": "total"},
    "monthly_average": {"label": "Monthly average", "metric": "average"},
}
CATEGORY_DEFAULT_PERIOD = "past_month"
TRANSACTIONS_PAGE_SIZE = 200
SAVINGS_CONFIDENCE_MODES = {"high", "balanced", "broad"}
SAVINGS_GROUPS = {"all", "subscriptions", "trends", "anomalies"}


def _uncategorised_category_filter():
    return (
        Q(classified_category__isnull=True)
        | Q(classified_category="")
        | Q(classified_category__iexact="Uncategorised")
    )


def _iter_json_paths(data, prefix=None, array_limit=5):
    if isinstance(data, dict):
        for key, value in data.items():
            next_prefix = f"{prefix}.{key}" if prefix else key
            yield from _iter_json_paths(value, next_prefix, array_limit)
    elif isinstance(data, list):
        for index, value in enumerate(data[:array_limit]):
            next_prefix = f"{prefix}[{index}]" if prefix else f"[{index}]"
            yield from _iter_json_paths(value, next_prefix, array_limit)
    else:
        if prefix:
            yield prefix


def collect_json_paths(limit=200):
    seen = set()
    collected = []
    queryset = FeedItem.objects.exclude(raw_json={}).order_by("-transaction_time")[:limit]
    for item in queryset:
        for path in _iter_json_paths(item.raw_json):
            if path not in seen:
                seen.add(path)
                collected.append(path)
    return collected


def _get_spending_category_options():
    options = list(
        Category.objects.filter(category_type="spending")
        .exclude(name__isnull=True)
        .exclude(name="")
        .values_list("name", flat=True)
        .distinct()
    )
    if "Uncategorised" not in options:
        options.append("Uncategorised")
    return options


def _next_rule_position():
    max_pos = ClassificationRule.objects.aggregate(Max("position"))["position__max"]
    return (max_pos or -1) + 1


def _get_rule_form_choices():
    category_options = _get_spending_category_options()

    space_options_query = (
        Category.objects.filter(category_type="space")
        .exclude(space_uid__isnull=True)
        .exclude(space_uid="")
        .values("space_uid", "name")
        .distinct()
    )
    space_choices = []
    for item in space_options_query:
        label = item["name"] or item["space_uid"]
        space_choices.append((item["space_uid"], label))

    json_path_options = collect_json_paths()
    return category_options, space_choices, json_path_options


def _prepare_quick_rule_form(form, locked_rule_type=None):
    hidden_fields = ["position", "reason", "space_uid", "json_path", "start_date", "end_date"]
    for field_name in hidden_fields:
        if field_name in form.fields:
            form.fields[field_name].widget = forms.HiddenInput()
    if locked_rule_type and locked_rule_type in RULE_TYPE_LABELS:
        label = RULE_TYPE_LABELS[locked_rule_type]
        form.fields["rule_type"].choices = [(locked_rule_type, label)]


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

    def __init__(
        self,
        *args,
        category_choices=None,
        space_choices=None,
        json_path_choices=None,
        **kwargs,
    ):
        self.category_choices = category_choices or []
        self.space_choices = space_choices or []
        self.json_path_choices = json_path_choices or []
        super().__init__(*args, **kwargs)

        self.fields["category"] = self._build_choice_field(
            "category",
            [(name, name) for name in sorted(set(self.category_choices))],
            "Select a category",
            "Category",
        )
        self.fields["space_uid"] = self._build_choice_field(
            "space_uid",
            [(value, label or value) for value, label in self.space_choices],
            "Any space",
            "Space UID",
            help_text="Limit the rule to a particular Starling space.",
        )
        self.fields["json_path"] = self._build_choice_field(
            "json_path",
            [(path, path) for path in self.json_path_choices],
            "Any JSON path",
            "JSON path",
            help_text="Dot-separated path in the stored transaction payload.",
        )

    def _build_choice_field(self, field_name, choices, default_label, label, help_text=None, required=False):
        option_list = [("", default_label)] + list(choices)
        if self.is_bound:
            current = self.data.get(field_name)
        else:
            current = self.initial.get(field_name) or getattr(self.instance, field_name, None)
        if current and current not in {v for v, _ in option_list}:
            option_list.append((current, current))
        field = forms.ChoiceField(choices=option_list, required=required, label=label)
        field.initial = current or ""
        if help_text:
            field.help_text = help_text
        return field

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

        if space_uid == "":
            space_uid = None
            cleaned["space_uid"] = None
        if json_path == "":
            json_path = None
            cleaned["json_path"] = None

        if rule_type in {"counterparty_regex", "space_name_regex", "source_regex"}:
            if not pattern:
                self.add_error("pattern", "Provide a regular expression to match against.")
            if not category:
                self.add_error("category", "Select the category to apply when the pattern matches.")

        if rule_type == "space":
            if not space_uid:
                self.add_error("space_uid", "Select the Starling space to match.")
            if not category:
                self.add_error("category", "Select the category to apply when the space matches.")

        if rule_type == "raw_path":
            if not json_path:
                self.add_error("json_path", "Select the JSON path to inspect.")
            if not category:
                self.add_error("category", "Select the category to apply when the JSON path has a value.")

        if rule_type == "starling_category" and category:
            cleaned["category"] = category.strip() or None

        return cleaned


@require_GET
def categories_overview(request):
    selected_period = request.GET.get("period")
    if selected_period not in CATEGORY_PERIODS:
        selected_period = CATEGORY_DEFAULT_PERIOD

    period_options = [
        {
            "key": key,
            "label": meta["label"],
            "metric": meta["metric"],
            "active": key == selected_period,
        }
        for key, meta in CATEGORY_PERIODS.items()
    ]

    context = {
        "periods": period_options,
        "default_period": selected_period,
        "data_endpoint": reverse("spaces:categories-data"),
    }
    return render(request, "spaces/categories.html", context)


@require_GET
def categories_data(request):
    period = request.GET.get("period") or CATEGORY_DEFAULT_PERIOD
    if period not in CATEGORY_PERIODS:
        return JsonResponse({"error": "Invalid period"}, status=400)

    reference = _parse_reference_time(request.GET.get("reference"))
    if reference is None:
        reference = datetime.now(timezone.utc)

    summary = summarise_category_totals(period=period, reference_time=reference)
    payload = _build_category_payload(summary, period)
    return JsonResponse(payload)


@require_GET
def spending(request, category_name=None, counterparty_name=None):
    return _render_cashflow_page(
        request,
        template_name="spaces/spending.html",
        base_view_name="spending",
        category_name=category_name,
        counterparty_name=counterparty_name,
    )


@require_GET
def income(request, category_name=None, counterparty_name=None):
    return _render_cashflow_page(
        request,
        template_name="spaces/income.html",
        base_view_name="income",
        category_name=category_name,
        counterparty_name=counterparty_name,
    )


@require_GET
def cashflow(request):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        _, _, days, _ = _resolve_time_window(request, default_days)
    except ValueError:
        days = default_days

    return render(
        request,
        "spaces/cashflow.html",
        {
            "summary_days": days,
            "category_options": _get_spending_category_options(),
            "transactions_page_size": TRANSACTIONS_PAGE_SIZE,
            "spending_page_url": reverse("spaces:spending"),
            "income_page_url": reverse("spaces:income"),
        },
    )


@require_GET
def savings(request):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        _, _, days, _ = _resolve_time_window(request, default_days)
    except ValueError:
        days = default_days

    return render(
        request,
        "spaces/savings.html",
        {
            "summary_days": days,
            "data_endpoint": reverse("spaces:savings-data"),
        },
    )


def _render_cashflow_page(request, *, template_name, base_view_name, category_name=None, counterparty_name=None):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        _, _, days, _ = _resolve_time_window(request, default_days)
    except ValueError:
        days = default_days

    search_query = (request.GET.get("search") or "").strip()

    category_options = _get_spending_category_options()
    counterparty_template = reverse(f"spaces:{base_view_name}-counterparty", args=["__counterparty__"])
    counterparty_base = counterparty_template.rsplit("__counterparty__", 1)[0]

    return render(
        request,
        template_name,
        {
            "summary_days": days,
            "initial_category": category_name or "",
            "initial_counterparty": counterparty_name or "",
            "initial_search": search_query,
            "base_spending_url": reverse(f"spaces:{base_view_name}"),
            "counterparty_base_url": counterparty_base,
            "category_options": category_options,
            "transactions_page_size": TRANSACTIONS_PAGE_SIZE,
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
def income_data(request):
    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        window_start, window_end, days, reference = _resolve_time_window(request, default_days)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    summary = calculate_income_by_category(
        days=days,
        reference_time=reference,
        start_time=window_start,
    )
    return JsonResponse(summary)


@require_GET
def cashflow_data(request):
    income_scope = (request.GET.get("income_scope") or "salary").strip().lower()
    if income_scope not in {"salary", "all"}:
        return JsonResponse({"error": "Invalid income scope"}, status=400)

    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        window_start, window_end, days, reference = _resolve_time_window(request, default_days)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    summary = calculate_monthly_cashflow_totals(
        days=days,
        reference_time=reference,
        start_time=window_start,
        income_scope=income_scope,
    )
    return JsonResponse(summary)


@require_GET
def savings_data(request):
    confidence_mode = (request.GET.get("confidence") or "balanced").strip().lower()
    if confidence_mode not in SAVINGS_CONFIDENCE_MODES:
        return JsonResponse({"error": "Invalid confidence mode"}, status=400)

    group = (request.GET.get("group") or "all").strip().lower()
    if group not in SAVINGS_GROUPS:
        return JsonResponse({"error": "Invalid group"}, status=400)

    default_days = max(settings.STARLING_SUMMARY_DAYS, 365)
    try:
        window_start, window_end, days, reference = _resolve_time_window(request, default_days)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    payload = calculate_savings_signals(
        days=days,
        reference_time=reference,
        start_time=window_start,
        confidence_mode=confidence_mode,
        group=group,
    )

    for signal in payload["signals"]:
        signal["drilldownUrl"] = _build_savings_drilldown_url(
            signal=signal,
            window_start=window_start,
            window_end=window_end,
        )

    return JsonResponse(payload)


@require_GET
def spending_transactions(request):
    return _cashflow_transactions(request, flow="spending")


@require_GET
def income_transactions(request):
    return _cashflow_transactions(request, flow="income")


@require_GET
def cashflow_transactions(request):
    flow = (request.GET.get("flow") or "both").strip().lower()
    income_scope = (request.GET.get("income_scope") or "salary").strip().lower()
    if income_scope not in {"salary", "all"}:
        return JsonResponse({"error": "Invalid income scope"}, status=400)
    return _cashflow_transactions(request, flow=flow, income_scope=income_scope)


@require_GET
def things_to_do(request):
    return render(
        request,
        "spaces/things_to_do.html",
        {
            "category_options": _get_spending_category_options(),
            "transactions_page_size": TRANSACTIONS_PAGE_SIZE,
        },
    )


@require_GET
def things_to_do_transactions(request):
    try:
        page = _parse_positive_int(request.GET.get("page"), 1)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    queryset = (
        FeedItem.objects.filter(_uncategorised_category_filter())
        .annotate(source_upper=Upper("source"))
        .exclude(source_upper__in=EXCLUDED_TRANSFER_SOURCES)
        .order_by("-transaction_time", "-feed_item_uid")
    )

    total_count = queryset.count()
    total_pages = max(1, math.ceil(total_count / TRANSACTIONS_PAGE_SIZE))
    if page > total_pages:
        return JsonResponse({"error": "page is out of range"}, status=400)
    page_start = (page - 1) * TRANSACTIONS_PAGE_SIZE
    page_end = page_start + TRANSACTIONS_PAGE_SIZE

    space_names = {
        (cat.account_uid, cat.category_uid): cat.name
        for cat in Category.objects.filter(category_type="space")
    }

    transactions = []
    for item in queryset[page_start:page_end]:
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
                "category": "Uncategorised",
                "raw": item.raw_json or {},
            }
        )

    return JsonResponse(
        {
            "count": len(transactions),
            "totalCount": total_count,
            "page": page,
            "pageSize": TRANSACTIONS_PAGE_SIZE,
            "totalPages": total_pages,
            "hasNextPage": page < total_pages,
            "hasPreviousPage": page > 1,
            "transactions": transactions,
        }
    )


def _cashflow_transactions(request, *, flow, income_scope="all"):
    if flow not in {"spending", "income", "both"}:
        return JsonResponse({"error": "Unsupported flow"}, status=400)
    if income_scope not in {"salary", "all"}:
        return JsonResponse({"error": "Unsupported income scope"}, status=400)

    category = request.GET.get("category")
    counterparty = request.GET.get("counterparty")
    search = (request.GET.get("search") or "").strip()
    try:
        page = _parse_positive_int(request.GET.get("page"), 1)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

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
        .order_by("-transaction_time", "-feed_item_uid")
    )
    if flow == "income":
        queryset = queryset.filter(amount_minor_units__gt=0)
        if income_scope == "salary":
            queryset = queryset.filter(classified_category__icontains="salary")
    elif flow == "spending":
        queryset = queryset.filter(amount_minor_units__lt=0)
    elif income_scope == "salary":
        queryset = queryset.filter(
            Q(amount_minor_units__lt=0)
            | (Q(amount_minor_units__gt=0) & Q(classified_category__icontains="salary"))
        )

    if category:
        if category == "Uncategorised":
            queryset = queryset.filter(_uncategorised_category_filter())
        else:
            queryset = queryset.filter(classified_category=category)

    if counterparty:
        queryset = queryset.filter(counterparty__iexact=counterparty)

    if search:
        amount_minor = _parse_amount_minor_units(search)
        search_filter = Q(counterparty__icontains=search)
        if amount_minor is not None:
            if flow == "spending":
                search_filter |= Q(amount_minor_units=-amount_minor) | Q(amount_minor_units=amount_minor)
            elif flow == "income":
                search_filter |= Q(amount_minor_units=amount_minor)
            else:
                search_filter |= Q(amount_minor_units=-amount_minor) | Q(amount_minor_units=amount_minor)
        queryset = queryset.filter(search_filter)

    total_count = queryset.count()
    total_pages = max(1, math.ceil(total_count / TRANSACTIONS_PAGE_SIZE))
    if page > total_pages:
        return JsonResponse({"error": "page is out of range"}, status=400)
    page_start = (page - 1) * TRANSACTIONS_PAGE_SIZE
    page_end = page_start + TRANSACTIONS_PAGE_SIZE

    space_names = {
        (cat.account_uid, cat.category_uid): cat.name
        for cat in Category.objects.filter(category_type="space")
    }

    transactions = []
    for item in queryset[page_start:page_end]:
        space_name = space_names.get((item.account_uid, item.space_uid))
        transactions.append(
            {
                "feedItemUid": item.feed_item_uid,
                "transactionTime": item.transaction_time.isoformat(),
                "counterparty": item.counterparty or "",
                "amountMinorUnits": int(
                    -item.amount_minor_units
                    if flow == "spending"
                    else (item.amount_minor_units if flow == "income" else item.amount_minor_units)
                ),
                "currency": item.currency or "GBP",
                "spaceUid": item.space_uid or "",
                "spaceName": space_name or "",
                "source": item.source or "",
                "classificationReason": item.classification_reason or "",
                "category": item.classified_category or "Uncategorised",
                "flow": "income" if item.amount_minor_units > 0 else "spending",
                "raw": item.raw_json or {},
            }
        )

    response = {
        "flow": flow,
        "incomeScope": income_scope,
        "reference": reference_time.isoformat(),
        "days": days,
        "start": window_start.isoformat(),
        "end": window_end.isoformat(),
        "count": len(transactions),
        "totalCount": total_count,
        "page": page,
        "pageSize": TRANSACTIONS_PAGE_SIZE,
        "totalPages": total_pages,
        "hasNextPage": page < total_pages,
        "hasPreviousPage": page > 1,
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
    category_options, space_choices, json_path_options = _get_rule_form_choices()
    selected_rule = None
    selected_rule_id = request.GET.get("rule")
    if selected_rule_id:
        selected_rule = ClassificationRule.objects.filter(pk=selected_rule_id).first()

    status = request.GET.get("status")
    message = {
        "created": "Classification rule created successfully.",
        "updated": "Classification rule updated successfully.",
        "deleted": "Classification rule deleted.",
        "applied": "Rules applied to existing transactions.",
        "apply-error": "Failed to apply rules. Check server logs for details.",
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

        form = ClassificationRuleForm(
            request.POST,
            instance=instance,
            category_choices=category_options,
            space_choices=space_choices,
            json_path_choices=json_path_options,
        )
        if form.is_valid():
            rule = form.save(commit=False)
            if rule.position is None:
                rule.position = _next_rule_position()
            rule.save()
            next_status = "updated" if instance else "created"
            return redirect(f"{reverse('spaces:classification-rules')}?status={next_status}&rule={rule.pk}")
    else:
        if selected_rule:
            form = ClassificationRuleForm(
                instance=selected_rule,
                category_choices=category_options,
                space_choices=space_choices,
                json_path_choices=json_path_options,
            )
        else:
            initial = {"position": _next_rule_position()}
            if request.GET.get("pattern"):
                initial["pattern"] = request.GET.get("pattern")
            pref_rule_type = request.GET.get("rule_type")
            if pref_rule_type in dict(RULE_TYPE_CHOICES):
                initial["rule_type"] = pref_rule_type
            form = ClassificationRuleForm(
                initial=initial,
                category_choices=category_options,
                space_choices=space_choices,
                json_path_choices=json_path_options,
            )

    context = {
        "rules": rules,
        "form": form,
        "selected_rule": selected_rule,
        "message": message,
        "rule_type_guidance": RULE_TYPE_CHOICES,
        "category_choices": sorted(set(category_options)),
    }
    return render(request, "spaces/classification_rules.html", context)


@require_POST
def apply_classification_rules(request):
    try:
        call_command("reclassify_transactions")
    except Exception:  # pragma: no cover - surface error message
        return redirect(f"{reverse('spaces:classification-rules')}?status=apply-error")
    return redirect(f"{reverse('spaces:classification-rules')}?status=applied")


@require_http_methods(["GET", "POST"])
def quick_classification_rule(request):
    category_options, space_choices, json_path_options = _get_rule_form_choices()
    locked_rule_type = request.GET.get("rule_type") or request.POST.get("rule_type")
    if locked_rule_type not in RULE_TYPE_LABELS:
        locked_rule_type = None
    apply_rules_selected = True
    if request.method == "POST":
        apply_rules_selected = bool(request.POST.get("apply_rules"))

    form_kwargs = {
        "category_choices": category_options,
        "space_choices": space_choices,
        "json_path_choices": json_path_options,
    }

    if request.method == "POST":
        form = ClassificationRuleForm(request.POST, **form_kwargs)
        _prepare_quick_rule_form(form, locked_rule_type)
        if form.is_valid():
            rule = form.save(commit=False)
            if rule.position is None:
                rule.position = _next_rule_position()
            rule.save()
            trigger_payload = {
                "ruleType": rule.rule_type,
                "pattern": rule.pattern or "",
                "category": rule.category or "",
                "applied": apply_rules_selected,
            }
            if apply_rules_selected:
                call_command("reclassify_transactions")
            response = HttpResponse(status=204)
            response["HX-Trigger"] = json.dumps({"rule-created": trigger_payload})
            return response
    else:
        initial = {"position": _next_rule_position()}
        if request.GET.get("pattern"):
            initial["pattern"] = request.GET.get("pattern")
        if locked_rule_type:
            initial["rule_type"] = locked_rule_type
        form = ClassificationRuleForm(initial=initial, **form_kwargs)
        _prepare_quick_rule_form(form, locked_rule_type)

    current_rule_type = (
        form.data.get("rule_type")
        if form.is_bound
        else form.initial.get("rule_type")
    )
    if current_rule_type not in RULE_TYPE_LABELS:
        current_rule_type = None

    pattern_value = form.data.get("pattern") if form.is_bound else form.initial.get("pattern", "")
    context = {
        "form": form,
        "pattern_value": pattern_value or "",
        "rule_type_label": RULE_TYPE_LABELS.get(current_rule_type),
        "apply_rules_checked": apply_rules_selected,
    }
    return render(request, "spaces/includes/quick_rule_form.html", context, status=200)


def _build_category_payload(summary, period_key):
    meta = CATEGORY_PERIODS[period_key]
    months = summary.get("months") or 0
    entries = []
    total_value_minor = 0.0
    for category_name, total_minor_units in summary.get("categories", []):
        total_minor_units = int(total_minor_units)
        if meta["metric"] == "average":
            if months > 0:
                value_minor = total_minor_units / months
            else:
                value_minor = 0.0
        else:
            value_minor = float(total_minor_units)
        entry = {
            "category": category_name,
            "totalMinorUnits": total_minor_units,
            "total": round(total_minor_units / 100, 2),
            "valueMinorUnits": value_minor,
            "value": round(value_minor / 100, 2),
        }
        entries.append(entry)
        total_value_minor += value_minor

    entries.sort(key=lambda item: item["valueMinorUnits"], reverse=True)

    if total_value_minor > 0:
        for item in entries:
            item["percentage"] = (item["valueMinorUnits"] / total_value_minor) * 100
    else:
        for item in entries:
            item["percentage"] = 0.0

    start = summary.get("start")
    end = summary.get("end")
    reference = summary.get("reference")

    return {
        "period": period_key,
        "metric": meta["metric"],
        "periodLabel": meta["label"],
        "reference": reference.isoformat() if reference else None,
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "categories": entries,
        "totalMinorUnits": float(total_value_minor),
        "total": round(total_value_minor / 100, 2),
        "months": months,
    }


def _build_savings_drilldown_url(*, signal, window_start, window_end):
    query = urlencode(
        {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
        }
    )

    counterparty = (signal.get("counterparty") or "").strip()
    if counterparty:
        base = reverse("spaces:spending-counterparty", args=[counterparty])
        return f"{base}?{query}"

    category = (signal.get("category") or "").strip()
    if category:
        base = reverse("spaces:spending-category", args=[category])
        return f"{base}?{query}"

    base = reverse("spaces:spending")
    return f"{base}?{query}"


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
    start = reference - timedelta(days=days)
    end = reference + timedelta(seconds=1)
    return start, end, days, reference
