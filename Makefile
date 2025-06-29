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
	uv run pytest --junitxml=junit/test-results.xml --cov=laktory --cov-report=xml --cov-report=html tests

coverage:
	open htmlcov/index.html

build:
	uv build

publish:
	uv publish

publishdoc:
	uv pip install 'mkdocs<1.6' 'mkdocstrings[python]' mkdocs-material mkdocs-video griffe_fieldz
	mkdocs gh-deploy --force