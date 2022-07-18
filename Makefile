# =============================================================================
PROJECT_NAME:=dpa

.DEFAULT_GOAL:=run
.SILENT:
SHELL:=/usr/bin/bash


# =============================================================================
# 			DEV
# =============================================================================
#
VENV_DIR:=.venv
VENV_BIN:=.venv/bin/
ACTIVATE:=source .venv/bin/activate &&

.PHONY: help setup run-service run-client proto lint format test build coverage

help:
	echo "Data-Parallel Actors\n"

setup:
	test -d $(VENV_DIR) || python3 -m venv $(VENV_DIR)
	poetry install

PROTO_DIR:=dpa/proto
proto:
	python -m grpc_tools.protoc -I$(PROTO_DIR) \
		--python_out=$(PROTO_DIR) \
		--grpc_python_out=$(PROTO_DIR) \
		dpa/proto/*.proto

run-service:
	python dpa/node.py

run-client:
	python dpa/client.py

lint:
	flake8 --show-source .
	bandit -q -r -c "pyproject.toml" .

format:
	black .

test:
	pytest

build:
	poetry build -q

clean:
	rm -rf $(VENV_DIR)
	find . -type d -name '__pycache__' -exec rm -rf {} +
# =============================================================================
