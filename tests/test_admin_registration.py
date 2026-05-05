import pytest
from django.contrib.admin.sites import site

from starling_web.spaces.models import (
    Category,
    CounterpartyNote,
    FeedItem,
    SavingsSignalDismissal,
    SyncState,
    TransactionNote,
)


@pytest.mark.django_db
def test_spaces_models_registered_with_admin():
    registry = site._registry
    assert Category in registry
    assert CounterpartyNote in registry
    assert FeedItem in registry
    assert SavingsSignalDismissal in registry
    assert SyncState in registry
    assert TransactionNote in registry
    feed_admin = registry[FeedItem]
    assert "counterparty" in feed_admin.list_display
    assert any(
        getattr(item, "__name__", "") == "FeedItemCategoryStateFilter"
        for item in feed_admin.list_filter
    )
