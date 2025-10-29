set shell := ['bash', '-lc']

alias default := test

clean:
    PYTHONPATH="${PYTHONPATH:-.}" UV_CACHE_DIR=.uv_cache uv run ruff check --fix starling_spaces tests
    UV_CACHE_DIR=.uv_cache uv run isort starling_spaces tests

test:
    PYTHONPATH="${PYTHONPATH:-.}" UV_CACHE_DIR=.uv_cache uv run pytest

report *args:
    PYTHONPATH="${PYTHONPATH:-.}" UV_CACHE_DIR=.uv_cache uv run python -m starling_spaces.cli {{args}}
