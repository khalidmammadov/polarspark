PY = python3.9

.PHONY: lint reformat build cleanup install test

lint:
	@PYTHON_EXECUTABLE=$(PY) ./bin/lint-python

reformat:
	@PYTHON_EXECUTABLE=$(PY) ./bin/reformat-python

build:
	@PYTHON_EXECUTABLE=$(PY) uv build

cleanup:
	./bin/cleanup.sh

install:
	$(MAKE) cleanup
	$(MAKE) build
	uv pip install --force-reinstall dist/*.whl

test:
	$(MAKE) cleanup
	pytest

remove-venv:
	rm -fR .venv

create-venv:
	uv venv --python 3.9


setup-dev:
	uv sync

bump-version:
	uv version --bump minor

publish-test:
	uv publish --index testpypi

publish-prod:
	uv publish


