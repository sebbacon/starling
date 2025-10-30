import json

import pytest
from django.test import Client, override_settings
from django.urls import reverse


pytestmark = pytest.mark.django_db


def test_homepage_displays_summary(sample_feed_database, settings):
    settings.STARLING_SUMMARY_DAYS = 5

    client = Client()
    response = client.get(reverse("spaces:home"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "Space One" in content
    assert "Average daily spend" in content
    assert "hx-get" in content
    assert "href=\"/spending/\"" in content


def test_summary_partial_returns_fragment(sample_feed_database, settings):

    client = Client()
    response = client.get(
        reverse("spaces:summary"),
        HTTP_HX_REQUEST="true",
    )

    assert response.status_code == 200
    assert "Space One" in response.content.decode()
    assert "<!DOCTYPE" not in response.content.decode()


def test_summary_partial_json_option(sample_feed_database, settings):

    client = Client()
    response = client.get(
        reverse("spaces:summary"),
        HTTP_ACCEPT="application/json",
    )

    assert response.status_code == 200
    payload = json.loads(response.content.decode())
    assert payload["spaces"]
