from __future__ import annotations

from django.urls import path

from . import views


app_name = "spaces"


urlpatterns = [
    path("", views.home, name="home"),
    path("summary/", views.summary, name="summary"),
    path("spending/", views.spending, name="spending"),
    path("spending/category/<str:category_name>/", views.spending, name="spending-category"),
    path("spending/data/", views.spending_data, name="spending-data"),
    path("spending/transactions/", views.spending_transactions, name="spending-transactions"),
]
