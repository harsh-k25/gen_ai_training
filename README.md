# ETL Pipeline - Windsurf Lab

An AI-assisted ETL pipeline built with Python, featuring data extraction, validation, transformation, and loading capabilities with Snowflake integration support.

## Features

- **Extract**: Read JSON data files
- **Validate**: 
  - Check required columns
  - Remove duplicate order IDs
  - Filter invalid region values
  - Remove negative amounts
- **Transform**: 
  - Replace null amounts with 0
  - Aggregate total revenue by region
- **Load**: 
  - Save to CSV (mock Snowflake)
  - Optional Snowflake integration
  - Partition output by region
  - Retry logic with exponential backoff

## Installation

```bash
pip install pandas pyyaml
# Optional for Snowflake integration:
pip install snowflake-connector-python
```

## Usage

### Basic Usage

```bash
python3 etl.py
```

### CLI Options

```bash
python3 etl.py --help
```

Available options:
- `--config`: Path to configuration file (default: config.yaml)
- `--input`: Override input file path
- `--output`: Override output file path
- `--log-level`: Override logging level (DEBUG, INFO, WARNING, ERROR)
- `--chunk-size`: Process data in chunks of this size (for large datasets)
- `--partition`: Partition output by region

### Examples

```bash
# Run with default config
python3 etl.py

# Use custom input file
python3 etl.py --input custom_orders.json

# Enable partitioned output
python3 etl.py --partition

# Process large datasets in chunks
python3 etl.py --chunk-size 10000

# Set debug logging
python3 etl.py --log-level DEBUG
```

## Configuration

Edit `config.yaml` to customize:

```yaml
input_file: orders.json
output_file: snowflake_mock.csv

valid_regions:
  - US
  - EU
  - APAC
  - LATAM
  - EMEA

logging:
  level: INFO

snowflake:
  enabled: false
  user: YOUR_USER
  password: YOUR_PASSWORD
  account: YOUR_ACCOUNT
  database: YOUR_DATABASE
  schema: YOUR_SCHEMA
  table: orders_summary
```

## Testing

Run pytest test cases:

```bash
pytest test_etl.py -v
```

## Output Files

- `snowflake_mock.csv`: Aggregated revenue by region (default output)
- `output_{region}.csv`: Partitioned output when using `--partition` flag

## Lab Components Completed

1. ✅ Project setup with dataset
2. ✅ Initial ETL pipeline with modular functions
3. ✅ Data quality checks (duplicates, invalid regions)
4. ✅ Config file integration (config.yaml)
5. ✅ CLI tool with argparse
6. ✅ Snowflake integration (optional)
7. ✅ Optimization for large datasets (chunk processing, vectorized operations)
8. ✅ Pytest test cases
9. ✅ Advanced enhancements (partition output, structured logging, retry logic)
