# Makefile for estat-master project

# Configuration
DATA_TYPE ?= jsic
REVISION ?= 04
OUTPUT_FORMAT ?= json
OUTPUT_DIR ?= data

# ============================================================================
# Main Targets
# ============================================================================

.PHONY: help
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Data retrieval:"
	@echo "  make create-jsic-master             # Create JSIC revision 04 master data (JSON)"
	@echo "  make create-jsic-master REVISION=03 # Create specific revision"
	@echo "  make create-jsic-master OUTPUT_FORMAT=csv  # Output as CSV"
	@echo ""
	@echo "Development:"
	@echo "  make install    # Install dependencies"
	@echo "  make test       # Run tests"
	@echo "  make lint       # Run linting"
	@echo "  make format     # Format code"
	@echo "  make clean      # Remove generated files"

.PHONY: create-master
create-master:
	@mkdir -p $(OUTPUT_DIR)
	uv run estat-master \
		--data-type $(DATA_TYPE) \
		--revision $(REVISION) \
		--output-format $(OUTPUT_FORMAT) \
		--output-path $(OUTPUT_DIR)/$(DATA_TYPE)_rev$(REVISION).$(OUTPUT_FORMAT)

.PHONY: create-jsic-master
create-jsic-master:
	@$(MAKE) create-master DATA_TYPE=jsic

# ============================================================================
# Development Targets
# ============================================================================

.PHONY: install
install:
	uv sync

.PHONY: test
test:
	uv run pytest -v

.PHONY: lint
lint:
	uv run ruff check .

.PHONY: format
format:
	uv run ruff format .

.PHONY: clean
clean:
	rm -rf $(OUTPUT_DIR)/*.json $(OUTPUT_DIR)/*.csv

# ============================================================================
# Extension Example for New Classification Types (e.g., JSOC)
# ============================================================================
# To add new classification types:
# 1. Add type to MasterDataType enum in src/estat_master/types.py
# 2. Implement ETL class inheriting from BaseETL
# 3. Update create_etl() factory in src/estat_master/cli/cli.py
# 4. Add convenience target here:
#
# .PHONY: create-jsoc-master
# create-jsoc-master:
#     @$(MAKE) create-master DATA_TYPE=jsoc
