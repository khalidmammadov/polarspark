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

changelog:
	@echo "# Changelog" > CHANGELOG.md.tmp
	@echo "" >> CHANGELOG.md.tmp
	@echo "## [$$(uv version --short)] - $$(date +%Y-%m-%d)" >> CHANGELOG.md.tmp
	@echo "### Added" >> CHANGELOG.md.tmp
	@git log "$$(git tag --sort=-creatordate | head -n 1)"..HEAD --pretty=format:"- %s" >> CHANGELOG.md.tmp
	@if [ -f CHANGELOG.md ]; then \
		echo "" >> CHANGELOG.md.tmp; \
		echo "" >> CHANGELOG.md.tmp; \
		tail -n +2 CHANGELOG.md >> CHANGELOG.md.tmp; \
	fi
	@mv CHANGELOG.md.tmp CHANGELOG.md
	@echo "Changelog updated in CHANGELOG.md"
