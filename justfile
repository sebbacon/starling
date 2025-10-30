set shell := ['bash', '-lc']

alias default := test

clean:
    PYTHONPATH="${PYTHONPATH:-.}" UV_CACHE_DIR=.uv_cache uv run ruff check --fix starling_spaces starling_web tests
    UV_CACHE_DIR=.uv_cache uv run isort starling_spaces starling_web tests

test:
    PYTHONPATH="${PYTHONPATH:-.}" UV_CACHE_DIR=.uv_cache uv run pytest

dev *args:
    UV_CACHE_DIR=.uv_cache uv run python starling_web/manage.py runserver {{args}}

report *args:
    PYTHONPATH="${PYTHONPATH:-.}" UV_CACHE_DIR=.uv_cache uv run python starling_web/manage.py report_spaces {{args}}

ingest *args:
    PYTHONPATH="${PYTHONPATH:-.}" UV_CACHE_DIR=.uv_cache uv run python starling_web/manage.py ingest_feeds {{args}}

average-spend *args:
    PYTHONPATH="${PYTHONPATH:-.}" UV_CACHE_DIR=.uv_cache uv run python starling_web/manage.py average_spend {{args}}

coverage:
    PYTHONPATH="${PYTHONPATH:-.}" UV_CACHE_DIR=.uv_cache uv run pytest --cov=starling_web --cov=starling_spaces --cov-report=term-missing

reclassify-transactions *args:
    PYTHONPATH="${PYTHONPATH:-.}" UV_CACHE_DIR=.uv_cache uv run python starling_web/manage.py reclassify_transactions {{args}}
