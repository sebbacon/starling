from __future__ import annotations

from django.urls import path
from django.views.generic import RedirectView

from . import views


app_name = "spaces"


urlpatterns = [
    path("", RedirectView.as_view(pattern_name="spaces:spending", permanent=False), name="home"),
    path("spending/", views.spending, name="spending"),
    path("spending/category/<str:category_name>/", views.spending, name="spending-category"),
    path("spending/counterparty/<str:counterparty_name>/", views.spending, name="spending-counterparty"),
    path("spending/data/", views.spending_data, name="spending-data"),
    path("spending/transactions/", views.spending_transactions, name="spending-transactions"),
    path("spending/transactions/recategorise/", views.recategorise_transactions, name="spending-recategorise"),
    path("rules/", views.manage_classification_rules, name="classification-rules"),
    path("rules/spaces/search/", views.space_lookup, name="space-lookup"),
    path("rules/json-path/search/", views.json_path_lookup, name="json-path-lookup"),
]
