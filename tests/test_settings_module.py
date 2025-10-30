import importlib.util
from pathlib import Path

import pytest
from django.core.exceptions import ImproperlyConfigured


SETTINGS_PATH = Path(__file__).resolve().parent.parent / "starling_web" / "starling_web" / "settings.py"


def _load_settings_with_env(env):
    spec = importlib.util.spec_from_file_location("temp_settings", SETTINGS_PATH)
    module = importlib.util.module_from_spec(spec)
    loader = spec.loader
    if loader is None:
        raise RuntimeError("Unable to load settings module")
    with pytest.MonkeyPatch.context() as patch:
        for key, default in {
            "DJANGO_SECRET_KEY": "secret",
            "DJANGO_DEBUG": "1",
            "DJANGO_ALLOWED_HOSTS": "localhost",
            "DJANGO_DATABASE_PATH": "test.sqlite3",
            "STARLING_FEEDS_DB": "data/starling_feeds.db",
            "STARLING_SUMMARY_DAYS": "30",
        }.items():
            value = env.get(key, default)
            if value is None:
                patch.delenv(key, raising=False)
            else:
                patch.setenv(key, value)
        loader.exec_module(module)
    return module


def test_settings_resolve_relative_paths():
    module = _load_settings_with_env({"DJANGO_DATABASE_PATH": "relative.sqlite3"})
    assert module.DATABASES["default"]["NAME"].name == "relative.sqlite3"


def test_settings_require_secret_key():
    with pytest.raises(ImproperlyConfigured):
        _load_settings_with_env({"DJANGO_SECRET_KEY": None})


def test_settings_reject_blank_values():
    with pytest.raises(ImproperlyConfigured):
        _load_settings_with_env({"DJANGO_SECRET_KEY": "   "})


def test_settings_validate_debug_flag():
    with pytest.raises(ImproperlyConfigured):
        _load_settings_with_env({"DJANGO_DEBUG": "maybe"})


def test_settings_require_allowed_hosts_when_debug_off():
    with pytest.raises(ImproperlyConfigured):
        _load_settings_with_env({"DJANGO_DEBUG": "0", "DJANGO_ALLOWED_HOSTS": ","})


def test_settings_validate_summary_days():
    with pytest.raises(ImproperlyConfigured):
        _load_settings_with_env({"STARLING_SUMMARY_DAYS": "0"})
