from __future__ import annotations

from django.urls import path
from django.views.generic import RedirectView

from . import views


app_name = "spaces"


urlpatterns = [
    path("", RedirectView.as_view(pattern_name="spaces:spending", permanent=False), name="home"),
    path("categories/", views.categories_overview, name="categories"),
    path("categories/data/", views.categories_data, name="categories-data"),
    path("categories/add/", views.add_category, name="categories-add"),
    path("spending/", views.spending, name="spending"),
    path("spending/category/<str:category_name>/", views.spending, name="spending-category"),
    path("spending/counterparty/<str:counterparty_name>/", views.spending, name="spending-counterparty"),
    path("spending/data/", views.spending_data, name="spending-data"),
    path("spending/transactions/", views.spending_transactions, name="spending-transactions"),
    path("spending/notes/counterparty/", views.save_counterparty_note, name="spending-counterparty-note"),
    path("spending/notes/transaction/", views.save_transaction_note, name="spending-transaction-note"),
    path("spending/transactions/recategorise/", views.recategorise_transactions, name="spending-recategorise"),
    path("income/", views.income, name="income"),
    path("income/category/<str:category_name>/", views.income, name="income-category"),
    path("income/counterparty/<str:counterparty_name>/", views.income, name="income-counterparty"),
    path("income/data/", views.income_data, name="income-data"),
    path("income/transactions/", views.income_transactions, name="income-transactions"),
    path("income/transactions/recategorise/", views.recategorise_transactions, name="income-recategorise"),
    path("cashflow/", views.cashflow, name="cashflow"),
    path("cashflow/data/", views.cashflow_data, name="cashflow-data"),
    path("cashflow/transactions/", views.cashflow_transactions, name="cashflow-transactions"),
    path("holidays/", views.holidays, name="holidays"),
    path("holidays/data/", views.holidays_data, name="holidays-data"),
    path("holidays/feedback/", views.holiday_feedback, name="holidays-feedback"),
    path("holidays/merchant-overrides/", views.holiday_merchant_overrides, name="holidays-merchant-overrides"),
    path("savings/", views.savings, name="savings"),
    path("savings/data/", views.savings_data, name="savings-data"),
    path("savings/dismissals/", views.savings_dismissals, name="savings-dismissals"),
    path("things-to-do/", views.things_to_do, name="things-to-do"),
    path("things-to-do/transactions/", views.things_to_do_transactions, name="things-to-do-transactions"),
    path("spending/rules/quick/", views.quick_classification_rule, name="classification-rules-quick"),
    path("rules/", views.manage_classification_rules, name="classification-rules"),
    path("rules/apply/", views.apply_classification_rules, name="classification-rules-apply"),
]
