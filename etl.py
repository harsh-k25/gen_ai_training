import pandas as pd
import logging
import yaml
import argparse
import time
from datetime import datetime

# Load configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Structured logging with timestamps
class StructuredFormatter(logging.Formatter):
    def format(self, record):
        timestamp = datetime.now().isoformat()
        return f"{timestamp} | {record.levelname} | {record.getMessage()}"

handler = logging.StreamHandler()
handler.setFormatter(StructuredFormatter())
logging.basicConfig(level=config["logging"]["level"], handlers=[handler])

def extract(chunk_size=None):
    logging.info("Extracting data...")
    if chunk_size:
        logging.info(f"Using chunked reading with size: {chunk_size}")
        return pd.read_json(config["input_file"], lines=True, chunksize=chunk_size)
    return pd.read_json(config["input_file"])

def validate(df):
    logging.info("Validating data...")
    required_cols = ["order_id", "customer_id", "amount", "region"]
    
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    # Check for duplicate order_id
    if df["order_id"].duplicated().any():
        duplicates = df[df["order_id"].duplicated()]["order_id"].tolist()
        logging.warning(f"Found duplicate order_ids: {duplicates}. Removing duplicates.")
        df = df.drop_duplicates(subset=["order_id"], keep="first")

    # Check for invalid region values
    valid_regions = set(config["valid_regions"])
    invalid_regions = df[~df["region"].isin(valid_regions)]["region"].unique()
    if len(invalid_regions) > 0:
        logging.warning(f"Found invalid regions: {invalid_regions}. Filtering out invalid records.")
        df = df[df["region"].isin(valid_regions)]

    # Remove negative amounts
    df = df[df["amount"].isnull() | (df["amount"] >= 0)]
    return df

def transform(df):
    logging.info("Transforming data...")
    # Vectorized operations for better performance
    df["amount"] = df["amount"].fillna(0)
    # Efficient grouping using vectorized aggregation
    return df.groupby("region", observed=True)["amount"].sum().reset_index()

def load(df, partition_by_region=False, use_snowflake=False, max_retries=3):
    logging.info("Loading data...")

    for attempt in range(max_retries):
        try:
            if use_snowflake:
                load_to_snowflake(df)
            else:
                # Mock Snowflake - save to CSV
                if partition_by_region:
                    # Partition output by region
                    for region in df["region"].unique():
                        region_df = df[df["region"] == region]
                        output_file = f"output_{region.lower()}.csv"
                        region_df.to_csv(output_file, index=False)
                        logging.info(f"Saved partition for region {region} to {output_file}")
                else:
                    df.to_csv(config["output_file"], index=False)
                    logging.info(f"Saved output to {config['output_file']}")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logging.warning(f"Load attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"Load failed after {max_retries} attempts: {e}")
                raise

def load_to_snowflake(df):
    """Load data to Snowflake using credentials from snowflake_config.json"""
    import json
    try:
        import snowflake.connector
    except ImportError:
        logging.warning("snowflake-connector-python not installed. Falling back to CSV.")
        df.to_csv(config["output_file"], index=False)
        return

    # Load credentials from snowflake_config.json
    with open("snowflake_config.json", "r") as f:
        sf_config = json.load(f)

    logging.info("Connecting to Snowflake...")

    conn = snowflake.connector.connect(
        user=sf_config["user"],
        password=sf_config["password"],
        account=sf_config["account"],
        database=sf_config.get("database"),
        schema=sf_config.get("schema"),
        warehouse=sf_config.get("warehouse"),
        role=sf_config.get("role")
    )

    cursor = conn.cursor()

    # Use database and schema
    cursor.execute(f"USE DATABASE {sf_config['database']}")
    cursor.execute(f"USE SCHEMA {sf_config['schema']}")

    # Create table if not exists
    table_name = sf_config.get("table", "orders_summary")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        region STRING,
        total_revenue FLOAT
    )
    """
    cursor.execute(create_table_sql)
    logging.info(f"Table {table_name} ready.")

    # Truncate and load pattern for fresh data
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    logging.info(f"Truncated table {table_name} for fresh load.")

    # Insert data
    for _, row in df.iterrows():
        cursor.execute(
            f"INSERT INTO {table_name} (region, total_revenue) VALUES (%s, %s)",
            (row['region'], row['amount'])
        )

    conn.commit()

    # Verify load
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    logging.info(f"Loaded {count} rows into {table_name}")

    cursor.close()
    conn.close()
    logging.info(f"Successfully loaded data to Snowflake table {sf_config['schema']}.{table_name}")

def run(config_file="config.yaml", chunk_size=None, partition_by_region=False, use_snowflake=False):
    global config
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    try:
        if chunk_size:
            # Process data in chunks for large datasets
            logging.info(f"Processing data in chunks of {chunk_size}")
            chunks = extract(chunk_size)
            aggregated_results = []

            for chunk in chunks:
                chunk = validate(chunk)
                chunk = transform(chunk)
                aggregated_results.append(chunk)

            # Combine all chunk results
            df = pd.concat(aggregated_results, ignore_index=True)
            # Final aggregation
            df = df.groupby("region")["amount"].sum().reset_index()
        else:
            df = extract()
            df = validate(df)
            df = transform(df)

        load(df, partition_by_region=partition_by_region, use_snowflake=use_snowflake)
        logging.info("ETL completed successfully")
    except Exception as e:
        logging.error(f"ETL failed: {e}")

def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline for processing order data")
    parser.add_argument("--config", type=str, default="config.yaml",
                        help="Path to configuration file (default: config.yaml)")
    parser.add_argument("--input", type=str, help="Override input file path")
    parser.add_argument("--output", type=str, help="Override output file path")
    parser.add_argument("--log-level", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Override logging level")
    parser.add_argument("--chunk-size", type=int,
                        help="Process data in chunks of this size (for large datasets)")
    parser.add_argument("--partition", action="store_true",
                        help="Partition output by region")
    parser.add_argument("--snowflake", action="store_true",
                        help="Load data to Snowflake using snowflake_config.json credentials")

    args = parser.parse_args()

    # Override config with CLI arguments if provided
    if args.input:
        config["input_file"] = args.input
    if args.output:
        config["output_file"] = args.output
    if args.log_level:
        config["logging"]["level"] = args.log_level
        logging.basicConfig(level=getattr(logging, args.log_level))

    run(args.config, chunk_size=args.chunk_size, partition_by_region=args.partition, use_snowflake=args.snowflake)

if __name__ == "__main__":
    main()
