# e-Stat Master Data Tool

A Python-based ETL tool for scraping and processing Japanese government classification master data from the e-Stat statistics portal.

## Supported Classifications

- **JSIC (日本標準産業分類)** - Japan Standard Industrial Classification
- **JSOC (日本標準職業分類)** - Japan Standard Occupational Classification *(coming soon)*
- Extensible architecture for adding more classification types

## Data Source

All classification data is sourced from the official e-Stat government statistics portal:
- **e-Stat Classifications**: https://www.e-stat.go.jp/classifications/terms
- Data includes official classification codes, names, descriptions, and examples

## Features

- Downloads classification master data from e-Stat API
- Scrapes classification examples and unsuitable examples from web pages
- Converts hierarchical classification structure to flat denormalized format
- Exports data in JSON or CSV format
- Supports multiple revisions for each classification type
- Schema validation using Pandera throughout the pipeline
- Extensible ETL architecture for adding new classification types

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd estat-master

# Install dependencies
make install
# or
uv sync
```

## Usage

### Quick Start (JSIC)

```bash
# Get latest JSIC master data (revision 04) as JSON
make create-jsic-master

# Get specific revision
make create-jsic-master REVISION=03

# Export as CSV
make create-jsic-master OUTPUT_FORMAT=csv
```

### CLI Usage

```bash
# JSIC (Japan Standard Industrial Classification)
uv run estat-master --data-type jsic --output-path output.json --revision 04

# With CSV output
uv run estat-master --data-type jsic --output-path output.csv --output-format csv --revision 04

# Debug mode (limit number of codes)
uv run estat-master --data-type jsic --output-path output.json --revision 04 --debug-code-limit 10

# Future: Other classification types (once implemented)
# uv run estat-master --data-type jsoc --output-path output.json --revision 06
```

### Available JSIC Revisions

- `04` - 2023-07-01 (latest)
- `03` - 2013-10-01
- `02` - 2007-11-01
- `01` - 2002-03-01

## Output Format

The tool outputs JSIC classification data with all hierarchy levels (Division → Major Group → Group → Class) in a flat structure:

```json
[
  {
    "division_code": "A",
    "division_code_name": "農業，林業",
    "major_group_code": "01",
    "major_group_code_name": "農業",
    "group_code": "011",
    "group_code_name": "耕種農業",
    "class_code": "0111",
    "class_code_name": "米作農業",
    "example": "米作農業の例...",
    "unsuitable_example": "不適当な例...",
    "release_date": "2023-07-01"
  }
]
```

## Development

```bash
# Run tests
make test

# Lint code
make lint

# Format code
make format

# Type check
uv run mypy src/ --ignore-missing-imports

# Run all CI checks locally
make test lint format

# Install pre-commit hooks
pre-commit install
```

## Project Structure

- `src/estat_master/` - Main source code
  - `cli/` - Command-line interface
  - `etl/` - ETL orchestration (base classes and implementations)
  - `extractor/` - Data extraction from e-Stat
  - `processor/` - Data transformation logic
  - `schema/` - Pandera schema definitions
- `tests/` - Test suite
- `data/` - Output directory for generated data files

## Architecture

The project follows an ETL (Extract, Transform, Load) pattern:

1. **Extract**: Downloads raw JSIC master data from e-Stat API and scrapes examples from web pages
2. **Transform**: Converts hierarchical classification to flat denormalized structure with forward-fill for parent codes
3. **Load**: Saves output to JSON or CSV format

Schema validation using Pandera ensures data integrity throughout the pipeline.

## License

MIT License - See LICENSE file for details
