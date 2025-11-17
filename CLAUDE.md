# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an e-Stat master data tool for scraping and processing Japanese government classification data. Currently supports JSIC (Japan Standard Industrial Classification / 日本標準産業分類) with plans to add JSOC (Japan Standard Occupational Classification / 日本標準職業分類) and other classification types. The project implements an extensible ETL pipeline to download, transform, and export classification master data with examples.

**Data Source**: All classification data is sourced from the official e-Stat government statistics portal: https://www.e-stat.go.jp/classifications/terms

## Development Commands

This project uses **uv** (not pip) for package management and task running.

### Setup
```bash
uv sync  # Install all dependencies including dev dependencies
# or
make install
```

### Makefile Targets (Recommended)
```bash
make create-jsic-master                    # Create JSIC revision 04 master data (JSON)
make create-jsic-master REVISION=03        # Create specific revision
make create-jsic-master OUTPUT_FORMAT=csv  # Output as CSV

# Development
make install    # Install dependencies
make test       # Run tests
make lint       # Run linting
make format     # Format code
make clean      # Remove generated files
```

### Running the ETL (Direct CLI)
```bash
# Basic usage
uv run estat-master --data-type jsic --output-path output.json --revision 04

# With CSV output
uv run estat-master --data-type jsic --output-path output.csv --output-format csv --revision 04

# Debug mode (limit number of codes to process)
uv run estat-master --data-type jsic --output-path output.json --revision 04 --debug-code-limit 10
```

### Testing
```bash
uv run pytest -v              # Run all tests with verbose output
uv run pytest tests/unittests/processor/test_jsic.py  # Run specific test file
uv run pytest -k test_name    # Run tests matching pattern
```

### Linting and Formatting
```bash
uv run ruff check .           # Check for linting errors
uv run ruff check . --fix     # Auto-fix linting errors
uv run ruff format .          # Format code
uv run ruff format --check .  # Check formatting without changes
```

### Type Checking
```bash
uv run mypy src/ --ignore-missing-imports
```

### Pre-commit Hooks
```bash
pre-commit install                # Install pre-commit hooks
pre-commit run --all-files        # Run all hooks manually
```

## Architecture

### ETL Pattern
The codebase follows a classic ETL (Extract, Transform, Load) pattern with generic base classes:

- **BaseETL[TIn, TOut]**: Abstract base class defining `extract()`, `transform()`, and `load()` methods
- **BaseETLConfig**: Configuration dataclass for ETL instances
- **Factory Pattern**: `create_etl()` in cli.py creates appropriate ETL instances based on data type

### JSIC Master ETL Flow

1. **Extract** (src/estat_master/etl/jsic.py:58-92)
   - Downloads raw JSIC master data from e-Stat API (src/estat_master/extractor/master_downloader.py)
   - Scrapes class examples and unsuitable examples from web pages (src/estat_master/extractor/example_extractor.py)
   - Includes 0.5s delay between requests to be polite to the server
   - Returns `JSICMasterETLIn` dataclass containing raw master data and examples

2. **Transform** (src/estat_master/etl/jsic.py:94-108)
   - Converts hierarchical classification structure to flat denormalized table (src/estat_master/processor/jsic.py)
   - JSIC hierarchy has 4 levels:
     - **Division (大分類)**: Single letter codes (A, B, C...)
     - **Major Group (中分類)**: 2-digit codes (01, 02...)
     - **Group (小分類)**: 3-digit codes (010, 011...)
     - **Class (細分類)**: 4-digit codes (0100, 0109...)
   - Uses forward-fill to propagate parent codes down the hierarchy
   - Merges class examples with master data
   - Returns `JSICMasterETLOut` with flattened data including all hierarchy levels for each class

3. **Load** (src/estat_master/etl/jsic.py:110-121)
   - Saves output to JSON or CSV format

### Schema Validation
Uses **Pandera** for runtime DataFrame schema validation throughout the pipeline:
- All schemas defined in src/estat_master/schema/
- Validation happens at ETL boundaries using `@pa.check_types` decorator
- Ensures data integrity from extraction through to final output

### Module Structure
- **cli/**: Click-based command-line interface
- **etl/**: ETL orchestration (base classes and JSIC implementation)
- **extractor/**: Data extraction from external sources
- **processor/**: Data transformation logic
- **schema/**: Pandera schema definitions
- **types.py**: Shared type definitions (enums, type aliases)

## Important Configuration

### Ruff Linting
- Complexity limit: max-complexity = 5 (keep functions simple)
- Line length: 120 characters
- Target: Python 3.13
- Docstrings and annotations not required (D and ANN rules disabled)

### JSIC Revisions
The codebase supports multiple JSIC revisions (defined in etl/jsic.py:20-24):
- 04: 2023-07-01
- 03: 2013-10-01
- 02: 2007-11-01
- 01: 2002-03-01

### Type Checking
mypy is configured but currently set to `continue-on-error: true` in CI (ci.yml:85), meaning type errors don't fail the build yet.

## Adding New Classification Types

To add support for a new classification type (e.g., JSOC):

1. Add the new type to `MasterDataType` enum in src/estat_master/types.py
2. Create schema definitions in src/estat_master/schema/
3. Implement extractor functions in src/estat_master/extractor/
4. Implement processor functions in src/estat_master/processor/
5. Create ETL class inheriting from `BaseETL[TIn, TOut]` in src/estat_master/etl/
6. Update `create_etl()` factory function in src/estat_master/cli/cli.py
7. Add Makefile target (e.g., `create-jsoc-master`) following the pattern in Makefile
8. Add tests in tests/unittests/ mirroring the source structure

## Testing Guidelines

Tests are organized by module in tests/unittests/ mirroring the source structure. When adding new functionality:
1. Add corresponding tests in the appropriate tests/unittests/ subdirectory
2. Use pytest fixtures defined in tests/conftest.py
3. Ensure tests run successfully with `uv run pytest -v`
