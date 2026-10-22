uv:
	curl -LsSf https://astral.sh/uv/install.sh | sh

install:
	uv sync

install-dev:
	uv sync --all-extras

install-with-dbks:
	uv sync --extra databricks

format-and-lint:
	ruff format ./
	ruff check ./

test:
	uv run pytest -m "not databricks_connect and not slow" --durations=20 --junitxml=junit/test-results.xml --cov=laktory --cov-report=xml --cov-report=html tests

test-slow:
	uv run pytest -m "slow and not databricks_connect" tests

coverage:
	open htmlcov/index.html

build:
	uv build

publish:
	uv publish

publishdoc:
	uv pip install -e ./
	uv pip install griffe_fieldz mkdocs mkdocs-video 'mkdocstrings[python]' mkdocs-material
	mkdocs gh-deploy --force