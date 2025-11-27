PYTHON_EXECUTABLE = python3.9

.PHONY: lint reformat

lint:
	@PYTHON_EXECUTABLE=$(PYTHON_EXECUTABLE) ./bin/lint-python

reformat:
	@PYTHON_EXECUTABLE=$(PYTHON_EXECUTABLE) ./bin/reformat-python
