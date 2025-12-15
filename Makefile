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
	uv run pytest

remove-venv:
	rm -fR .venv

create-venv:
	uv venv --python 3.9

setup-dev:
	uv sync

tag-push-release:
	git add .
	VERSION=$(uv version --short)
	git commit -S -m "chore(release): v${VERSION}"
	git tag v$(uv version --short)
	git push origin v$(uv version --short)

# To test the build
release:
	uv version --bump patch
	$(MAKE) tag-push-release

release-candidate:
	uv version --bump rc --bump patch
	$(MAKE) tag-push-release
