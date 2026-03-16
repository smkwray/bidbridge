PYTHON ?= python

.PHONY: install doctor list-sources demo-data demo-panel verify test

install:
	$(PYTHON) -m pip install -e .[dev]

doctor:
	$(PYTHON) -m bidbridge doctor

list-sources:
	$(PYTHON) -m bidbridge list-sources

demo-data:
	$(PYTHON) scripts/make_demo_data.py

demo-panel:
	$(PYTHON) scripts/build_demo_panel.py

verify:
	$(PYTHON) scripts/verify_repo.py

test:
	pytest
