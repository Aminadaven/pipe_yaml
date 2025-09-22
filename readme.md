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
- Java 17+ (for Spark)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/aminadaven/pipe_yaml.git
   cd pipe_yaml
   ```

2. Create and activate a virtual environment using uv:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # On Windows
   # or
   source .venv/bin/activate  # On Unix/macOS
   ```

3. Install dependencies:
   ```bash
   pip install -e .
   ```

## Usage

1. Create a YAML configuration file (e.g., `config.yaml`):

   ```yaml
   pipeline: sales_etl
   engine: duckdb  # or 'spark'
   
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
       format: csv
       path: "output/sales.csv"
   ```

2. Run the ETL pipeline:
   ```bash
   python __main__.py pipeline/sales_etl.yaml
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
- `options`: Additional options for the data source (e.g., `header: true` for CSV)

### Transform Configuration

- `step`: Unique identifier for the transformation step
- `sql`: SQL query to execute
- `df`: (Optional) Name to store the result DataFrame

### Load Configuration

- `step`: Unique identifier for the load step
- `df`: Name of the DataFrame to save
- `path`: Output path
- `format`: Output format (e.g., `csv`, `parquet`)
- `mode`: Write mode (e.g., `overwrite`, `append`)
- `options`: Additional options for the output format

## Examples

### Basic ETL with CSV

```yaml
pipeline: basic_etl
engine: duckdb

extract:
  - step: load_data
    df: my_data
    path: "data/input.csv"
    format: csv
    options:
      header: true

transform:
  - step: process_data
    sql: SELECT *, UPPER(column1) as upper_column FROM my_data
    df: processed_data

load:
  - step: save_results
    df: processed_data
    path: "output/result.parquet"
    format: parquet
    mode: overwrite
```

