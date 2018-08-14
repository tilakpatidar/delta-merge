BASH_ENV := .env
SHELL := /bin/bash

clean-pyc:
	find ./scripts -name '*.pyc' -exec rm -rf {} +
	find ./test -name '*.pyc' -exec rm -rf {} +
	find ./scripts -name '*.pyo' -exec rm -rf {} +
	find ./test -name '*.pyo' -exec rm -rf {} +
	find ./scripts -name '__pycache__' -exec rm -rf {} +
	find ./test -name '__pycache__' -exec rm -rf {} +

lint:
	flake8 --exclude=$(LINT_EXCLUDE)

clean-build: clean-pyc
	rm -rf build/
	rm -rf builds/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache

test: clean-pyc clean-build
	py.test --verbose --color=yes $(TEST_PATH) test/

setup:
	build_scripts/setup.sh

clean-example:
	rm -rf examples/costs_bad examples/costs_delta examples/costs_history/file_id=20180727T121212.SNAPSHOT examples/costs_snapshot examples/costs_history/._SUCCESS.crc examples/costs_history/_SUCCESS