# ETL Pipeline with YAML Configuration

A flexible ETL (Extract, Transform, Load) pipeline framework that supports multiple processing engines (Spark and
DuckDB) with YAML-based configuration.

## Features

- **Multiple Engine Support**: Choose between Apache Spark or DuckDB as the processing engine
- **YAML Configuration**: Define your ETL workflows in simple YAML files
- **Flexible Data Sources**: Support for various data formats including CSV and JSON
- **SQL-based Transformations**: Write SQL queries for data transformations
- **Easy to Extend**: Add support for new data sources and transformations

## Requirements

- Python 3.13+
- Java 17+ (for Spark engine)
- [uv](https://github.com/astral-sh/uv) package manager

## Installation

1. **Install uv** (if not already installed):
   ```bash
   # On Windows
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

   # On macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone the repository**:
   ```bash
   git clone https://github.com/aminadaven/pipe_yaml.git
   cd pipe_yaml
   ```

3. **Set up the project**:
   ```bash
   # Install the package in development mode with all dependencies
   uv sync --dev

   # Activate the virtual environment
   # On Windows
   .venv\Scripts\activate

   # On macOS/Linux
   source .venv/bin/activate
   ```

## Usage

### Running the ETL Tool

The project provides multiple ways to run ETL pipelines:

1. **Using the etl command** (recommended):
   ```bash
   etl pipelines/sales_etl.yaml
   ```

2. **Using Python module**:
   ```bash
   python -m pipe_yaml pipelines/sales_etl.yaml
   ```

3. **Using the installed script**:
   ```bash
   python -m pipe_yaml.__main__ pipelines/sales_etl.yaml
   ```

### Creating Your First Pipeline

1. Create a YAML configuration file (e.g., `my_pipeline.yaml`):

```yaml
pipeline: my_etl
engine: duckdb  # or 'spark'

extract:
  - step: load_data
    df: source_data
    path: "data/input.csv"
    format: csv
    options:
      header: true

transform:
  - step: process_data
    sql: SELECT *, UPPER(column1) as upper_column FROM source_data
    df: processed_data

load:
  - step: save_results
    df: processed_data
    path: "output/result.parquet"
    format: parquet
    mode: overwrite
```

2. Run the pipeline:
   ```bash
   etl my_pipeline.yaml
   ```

## Configuration Options

### Pipeline Configuration

- `pipeline`: Name of the pipeline (for logging purposes)
- `engine`: Processing engine to use (`spark` or `duckdb`)

### Extract Configuration

- `step`: Unique identifier for the extraction step
- `df`: Name of the DataFrame to store the extracted data
- `path`: Path to the input file
- `format`: Input format (e.g., `csv`, `json`)
- `cache`: (Optional) Cache the data for better performance (default: false)
- `options`: Additional options for the data source (e.g., `header: true` for CSV)

### Transform Configuration

- `step`: Unique identifier for the transformation step
- `sql`: SQL query to execute
- `sql_file`: (Optional) Path to a SQL file containing the query template
- `inputs`: (Optional) Dictionary mapping input DataFrame names to pipeline DataFrame names
- `df`: (Optional) Name to store the result DataFrame

### Load Configuration

- `step`: Unique identifier for the load step
- `df`: Name of the DataFrame to save
- `path`: Output path
- `format`: Output format (e.g., `csv`, `parquet`)
- `mode`: Write mode (e.g., `overwrite`, `append`)
- `options`: Additional options for the output format

## Examples

### Basic ETL with CSV Data

```yaml
pipeline: basic_csv_etl
engine: duckdb

extract:
  - step: load_sales_data
    df: sales_data
    path: "data/sales.csv"
    format: csv
    options:
      header: true

transform:
  - step: clean_sales
    sql: SELECT SUPPLIER, "ITEM TYPE", sum("WAREHOUSE SALES") AS SALES
         FROM sales_data
         GROUP BY SUPPLIER, "ITEM TYPE"
    df: clean_sales

load:
  - step: sales_output
    df: clean_sales
    mode: overwrite
    format: parquet
    path: "output/sales.zstd.parquet"
    options:
      compression: zstd
```

### ETL with JSON Data and SQL File

```yaml
pipeline: wine_reviews_etl
engine: duckdb

extract:
  - step: load_winemag_data
    df: winemag_data
    path: "data/winemag-reviews.json"
    format: json

transform:
  - step: most_reviewed
    sql_file: sql/most_reviewed.sql
    inputs:
      winemag_df: winemag_data
    df: most_reviewed

load:
  - step: winemag_output
    df: winemag_data
    mode: overwrite
    format: parquet
    path: "output/winemag.zstd.parquet"
    options:
      compression: zstd
```

## Testing

Run the test suite using pytest:

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=src/pipe_yaml --cov-report=html

# Run specific test file
pytest tests/test_models.py
```

## Development

### Development Dependencies

The project uses several development tools that are automatically installed when you run `uv sync --dev`:

- **pytest**: Testing framework
- **black**: Code formatter
- **isort**: Import sorter
- **ruff**: Fast Python linter
- **mypy**: Static type checker

### Code Quality

Format your code:

```bash
black src/ tests/
isort src/ tests/
```

Lint your code:

```bash
ruff check src/ tests/
```

Type check your code:

```bash
mypy src/
```

## Project Structure

```
pipe_yaml/
├── src/pipe_yaml/        # Source code
├── tests/               # Test files
├── pipelines/           # Example pipeline configurations
├── data/                # Sample data files
├── sql/                 # SQL files for transformations
├── output/              # Generated output files
└── pyproject.toml       # Project configuration
