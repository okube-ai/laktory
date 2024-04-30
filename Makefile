install:
	pip install ./

dev:
	pip install -e './[dev,test,spark,azure,aws,gcp]' 'pulumi==3.113.3' 'pulumi_databricks==1.36.0'

test:
	pytest --junitxml=junit/test-results.xml --cov=laktory --cov-report=xml --cov-report=html tests

coverage:
	open htmlcov/index.html

build:
	pip install build
	python -m build

publish:
	pip install build twine
	python -m build
	twine upload dist/*

publishdoc:
	pip install 'mkdocs<1.6' mkdocstrings[python] mkdocs-material mkdocs-video
	mkdocs gh-deploy --force