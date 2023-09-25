install:
	pip install ./

dev:
	pip install -e './[dev,test,azure,aws,gcp]'

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
