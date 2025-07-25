# Makefile for fabric-data-transformation

PYTHON := python3
VENV := .venv
VENV_ACTIVATE := $(VENV)/bin/activate

# Default target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  make setup    - Create venv and install dependencies"
	@echo "  make run      - Run the CLI (usage: make run ARGS='path/to/models --output ./output')"
	@echo "  make test     - Run tests"
	@echo "  make lint     - Run linters"
	@echo "  make clean    - Remove venv and cache files"

# Setup virtual environment
$(VENV_ACTIVATE):
	$(PYTHON) -m venv $(VENV)
	. $(VENV_ACTIVATE) && pip install --upgrade pip setuptools wheel
	. $(VENV_ACTIVATE) && pip install -e ".[dev]"
	@echo "✅ Setup complete! Use 'make run ARGS=...' to run the CLI"

.PHONY: setup
setup: $(VENV_ACTIVATE)

# Run the CLI
.PHONY: run
run: $(VENV_ACTIVATE)
	@. $(VENV_ACTIVATE) && data-transform $(ARGS)

# Run tests
.PHONY: test
test: $(VENV_ACTIVATE)
	@. $(VENV_ACTIVATE) && pytest

# Run linters
.PHONY: lint
lint: $(VENV_ACTIVATE)
	@. $(VENV_ACTIVATE) && black src/ && flake8 src/ && mypy src/data_transformation_tool/

# Clean up
.PHONY: clean
clean:
	rm -rf $(VENV)
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .mypy_cache .pytest_cache htmlcov .coverage

# Convenience targets
.PHONY: format
format: $(VENV_ACTIVATE)
	@. $(VENV_ACTIVATE) && black src/

.PHONY: typecheck
typecheck: $(VENV_ACTIVATE)
	@. $(VENV_ACTIVATE) && mypy src/data_transformation_tool/