from __future__ import annotations

from django.urls import path

from . import views


app_name = "spaces"


urlpatterns = [
    path("", views.home, name="home"),
    path("summary/", views.summary, name="summary"),
]
