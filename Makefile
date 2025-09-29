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
	uv run pytest -m "not dbks" --junitxml=junit/test-results.xml --cov=laktory --cov-report=xml --cov-report=html tests

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