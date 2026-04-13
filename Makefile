PYTHON ?= python3

.PHONY: setup run test seed token fmt-check

setup:
	uv venv .venv
	. .venv/bin/activate && uv pip install -r requirements-dev.txt

run:
	. .venv/bin/activate && uvicorn main:app --host 0.0.0.0 --port 8080

test:
	. .venv/bin/activate && PYTHONPATH=. pytest -q

seed:
	. .venv/bin/activate && PYTHONPATH=. $(PYTHON) scripts/seed_demo.py

token:
	. .venv/bin/activate && PYTHONPATH=. $(PYTHON) scripts/generate_auth_token.py --sub demo-user --role admin

fmt-check:
	. .venv/bin/activate && $(PYTHON) -m compileall -q .
