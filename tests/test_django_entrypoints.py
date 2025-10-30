import importlib
import importlib.util
from pathlib import Path

import pytest


def test_manage_main_invokes_execute(monkeypatch):
    calls = {}

    def fake_execute(argv):
        calls["argv"] = list(argv)

    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "starling_web.starling_web.settings")
    monkeypatch.setattr("django.core.management.execute_from_command_line", fake_execute)
    monkeypatch.setattr("sys.argv", ["manage.py", "check"])

    module = importlib.import_module("starling_web.manage")
    importlib.reload(module)
    module.main()

    assert calls["argv"] == ["manage.py", "check"]


def test_asgi_application_available():
    module = importlib.import_module("starling_web.starling_web.asgi")
    assert module.application is not None


def test_wsgi_application_available():
    module = importlib.import_module("starling_web.starling_web.wsgi")
    assert module.application is not None


def test_manage_main_handles_import_errors(monkeypatch):
    module = importlib.import_module("starling_web.manage")
    original_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "django.core.management":
            raise ImportError("boom")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)

    with pytest.raises(ImportError):
        module.main()


def test_manage_module_executes_main(monkeypatch):
    calls = {}

    def fake_execute(argv):
        calls["argv"] = list(argv)

    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "starling_web.starling_web.settings")
    monkeypatch.setattr("django.core.management.execute_from_command_line", fake_execute)
    monkeypatch.setattr("sys.argv", ["manage.py", "check"])

    import runpy

    runpy.run_module("starling_web.manage", run_name="__main__")

    assert calls["argv"] == ["manage.py", "check"]


def test_manage_main_works_with_script_directory(monkeypatch):
    manage_path = Path(__file__).resolve().parent.parent / "starling_web" / "manage.py"
    spec = importlib.util.spec_from_file_location("tmp_manage", manage_path)
    module = importlib.util.module_from_spec(spec)
    loader = spec.loader
    if loader is None:
        pytest.skip("Cannot load manage module")

    calls = {}

    def fake_execute(argv):
        calls["argv"] = list(argv)

    monkeypatch.setattr("django.core.management.execute_from_command_line", fake_execute)
    monkeypatch.setattr("sys.argv", ["manage.py", "check"])
    monkeypatch.setattr("sys.path", [str(manage_path.parent)])

    loader.exec_module(module)
    module.main()

    assert calls["argv"] == ["manage.py", "check"]
