.PHONY: lint reformat build cleanup install test

lint:
	@uv run ./bin/lint-python

reformat:
	@uv run ./bin/reformat-python

build:
	@uv run build

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
	git commit -S -m "chore(release): v$(uv version --short)"
	git tag v$(uv version --short)
	git push origin v$(uv version --short)

# To test the build
release:
	uv version --bump patch
	$(MAKE) tag-push-release

release-candidate:
	uv version --bump rc --bump patch
	$(MAKE) tag-push-release
