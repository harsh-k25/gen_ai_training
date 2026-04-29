"""
ETL Pipeline - Multi-Source Order Analytics
=============================================
Processes 4 data sources into a star schema:
  - customers.csv   → dim_customers
  - products.csv    → dim_products
  - orders.json + order_items.csv → fact_orders
  - Derived aggregation           → fact_orders_summary

Usage:
    python etl.py                          # Mock mode (CSV output)
    python etl.py --snowflake              # Load to Snowflake
    python etl.py --partition              # Partition fact output by region
    python etl.py --log-level DEBUG        # Verbose logging
"""

import pandas as pd
import logging
import yaml
import argparse
import json
import time
import os
import sys
from datetime import datetime


# ─── Structured Logging ─────────────────────────────────────────────────────────

class StructuredFormatter(logging.Formatter):
    def format(self, record):
        timestamp = datetime.now().isoformat()
        return f"{timestamp} | {record.levelname} | {record.getMessage()}"


handler = logging.StreamHandler()
handler.setFormatter(StructuredFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)


# ─── Configuration ──────────────────────────────────────────────────────────────

def load_config(config_file: str = "config.yaml") -> dict:
    """Load pipeline configuration from YAML."""
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


# ═══════════════════════════════════════════════════════════════════════════════
#  EXTRACT PHASE
# ═══════════════════════════════════════════════════════════════════════════════

def extract_orders(config: dict) -> pd.DataFrame:
    """Extract orders from JSON file."""
    filepath = config["orders_file"]
    logger.info(f"Extracting orders from: {filepath}")
    df = pd.read_json(filepath)
    logger.info(f"  → Extracted {len(df)} order records")
    return df


def extract_customers(config: dict) -> pd.DataFrame:
    """Extract customers from CSV file."""
    filepath = config["customers_file"]
    logger.info(f"Extracting customers from: {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"  → Extracted {len(df)} customer records")
    return df


def extract_products(config: dict) -> pd.DataFrame:
    """Extract products from CSV file."""
    filepath = config["products_file"]
    logger.info(f"Extracting products from: {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"  → Extracted {len(df)} product records")
    return df


def extract_order_items(config: dict) -> pd.DataFrame:
    """Extract order items from CSV file."""
    filepath = config["order_items_file"]
    logger.info(f"Extracting order items from: {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"  → Extracted {len(df)} order item records")
    return df


def extract_all(config: dict) -> dict:
    """Extract all data sources and return as a dict of DataFrames."""
    logger.info("=" * 60)
    logger.info("EXTRACT PHASE")
    logger.info("=" * 60)
    return {
        "orders": extract_orders(config),
        "customers": extract_customers(config),
        "products": extract_products(config),
        "order_items": extract_order_items(config),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  VALIDATE PHASE
# ═══════════════════════════════════════════════════════════════════════════════

def validate_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Validate customer data."""
    logger.info("Validating customers...")
    initial_count = len(df)

    required_cols = ["customer_id", "customer_name", "email", "region", "tier"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column in customers: {col}")

    # Remove duplicates on customer_id
    if df["customer_id"].duplicated().any():
        dups = df[df["customer_id"].duplicated()]["customer_id"].tolist()
        logger.warning(f"  Duplicate customer_ids found: {dups}. Keeping first occurrence.")
        df = df.drop_duplicates(subset=["customer_id"], keep="first")

    # Remove rows with null customer_id or customer_name
    df = df.dropna(subset=["customer_id", "customer_name"])

    # Normalize email to lowercase
    df["email"] = df["email"].str.lower().str.strip()

    # Normalize tier
    valid_tiers = {"Gold", "Silver", "Bronze", "Platinum"}
    invalid_tiers = df[~df["tier"].isin(valid_tiers)]["tier"].unique()
    if len(invalid_tiers) > 0:
        logger.warning(f"  Invalid tiers found: {invalid_tiers}. Setting to 'Bronze'.")
        df.loc[~df["tier"].isin(valid_tiers), "tier"] = "Bronze"

    logger.info(f"  ✓ Customers validated: {initial_count} → {len(df)} records")
    return df


def validate_products(df: pd.DataFrame) -> pd.DataFrame:
    """Validate product data."""
    logger.info("Validating products...")
    initial_count = len(df)

    required_cols = ["product_id", "product_name", "category", "unit_price"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column in products: {col}")

    # Remove duplicates on product_id
    if df["product_id"].duplicated().any():
        dups = df[df["product_id"].duplicated()]["product_id"].tolist()
        logger.warning(f"  Duplicate product_ids found: {dups}. Keeping first occurrence.")
        df = df.drop_duplicates(subset=["product_id"], keep="first")

    # Remove rows with null product_id or product_name
    df = df.dropna(subset=["product_id", "product_name"])

    # Validate unit_price is positive
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
    neg_prices = df[df["unit_price"] < 0]
    if len(neg_prices) > 0:
        logger.warning(f"  Found {len(neg_prices)} products with negative prices. Setting to 0.")
        df.loc[df["unit_price"] < 0, "unit_price"] = 0

    logger.info(f"  ✓ Products validated: {initial_count} → {len(df)} records")
    return df


def validate_orders(df: pd.DataFrame, valid_regions: list) -> pd.DataFrame:
    """Validate order data."""
    logger.info("Validating orders...")
    initial_count = len(df)

    required_cols = ["order_id", "customer_id", "amount", "region"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column in orders: {col}")

    # Remove duplicate order_ids
    if df["order_id"].duplicated().any():
        dups = df[df["order_id"].duplicated()]["order_id"].tolist()
        logger.warning(f"  Duplicate order_ids found: {dups}. Keeping first occurrence.")
        df = df.drop_duplicates(subset=["order_id"], keep="first")

    # Validate regions
    valid_set = set(valid_regions)
    invalid = df[~df["region"].isin(valid_set)]["region"].unique()
    if len(invalid) > 0:
        logger.warning(f"  Invalid regions found: {invalid}. Filtering out.")
        df = df[df["region"].isin(valid_set)]

    # Remove negative amounts
    neg_amounts = df[df["amount"] < 0] if df["amount"].notna().any() else pd.DataFrame()
    if len(neg_amounts) > 0:
        logger.warning(f"  Found {len(neg_amounts)} orders with negative amounts. Removing.")
        df = df[df["amount"].isna() | (df["amount"] >= 0)]

    # Fill null amounts with 0
    df["amount"] = df["amount"].fillna(0)

    logger.info(f"  ✓ Orders validated: {initial_count} → {len(df)} records")
    return df


def validate_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """Validate order items data."""
    logger.info("Validating order items...")
    initial_count = len(df)

    required_cols = ["order_item_id", "order_id", "product_id", "quantity", "discount_pct"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column in order_items: {col}")

    # Remove duplicates on order_item_id
    if df["order_item_id"].duplicated().any():
        dups = df[df["order_item_id"].duplicated()]["order_item_id"].tolist()
        logger.warning(f"  Duplicate order_item_ids found: {dups}. Keeping first occurrence.")
        df = df.drop_duplicates(subset=["order_item_id"], keep="first")

    # Validate quantity is positive
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df = df[df["quantity"] > 0]

    # Validate discount_pct is 0–1 range
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0)
    df.loc[df["discount_pct"] < 0, "discount_pct"] = 0
    df.loc[df["discount_pct"] > 1, "discount_pct"] = 1

    logger.info(f"  ✓ Order items validated: {initial_count} → {len(df)} records")
    return df


def validate_all(data: dict, config: dict) -> dict:
    """Validate all extracted data."""
    logger.info("=" * 60)
    logger.info("VALIDATE PHASE")
    logger.info("=" * 60)
    valid_regions = config.get("valid_regions", ["US", "EU", "APAC"])
    return {
        "customers": validate_customers(data["customers"]),
        "products": validate_products(data["products"]),
        "orders": validate_orders(data["orders"], valid_regions),
        "order_items": validate_order_items(data["order_items"]),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  TRANSFORM PHASE
# ═══════════════════════════════════════════════════════════════════════════════

def transform_dim_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Transform customers into dimension table format."""
    logger.info("Transforming dim_customers...")
    df = df.copy()
    df["customer_name"] = df["customer_name"].str.strip().str.title()
    df["region"] = df["region"].str.upper().str.strip()
    df["tier"] = df["tier"].str.strip().str.title()
    df["loaded_at"] = datetime.now()
    logger.info(f"  ✓ dim_customers: {len(df)} records ready")
    return df[["customer_id", "customer_name", "email", "region", "tier", "loaded_at"]]


def transform_dim_products(df: pd.DataFrame) -> pd.DataFrame:
    """Transform products into dimension table format."""
    logger.info("Transforming dim_products...")
    df = df.copy()
    df["product_name"] = df["product_name"].str.strip().str.title()
    df["category"] = df["category"].str.strip().str.title()
    df["loaded_at"] = datetime.now()
    logger.info(f"  ✓ dim_products: {len(df)} records ready")
    return df[["product_id", "product_name", "category", "unit_price", "loaded_at"]]


def transform_fact_orders(
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join orders + order_items + products to build fact_orders at line-item grain.
    Computes: line_total = quantity × unit_price × (1 - discount_pct)
    """
    logger.info("Transforming fact_orders...")

    # Join order_items with orders to get customer_id and region
    fact = order_items.merge(
        orders[["order_id", "customer_id", "region"]],
        on="order_id",
        how="inner",
    )

    # Join with products to get unit_price
    fact = fact.merge(
        products[["product_id", "unit_price"]],
        on="product_id",
        how="left",
    )

    # Compute line_total
    fact["unit_price"] = fact["unit_price"].fillna(0)
    fact["line_total"] = (
        fact["quantity"] * fact["unit_price"] * (1 - fact["discount_pct"])
    ).round(2)

    fact["loaded_at"] = datetime.now()

    logger.info(f"  ✓ fact_orders: {len(fact)} line items ready")
    return fact[
        [
            "order_item_id", "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "discount_pct", "line_total",
            "region", "loaded_at",
        ]
    ]


def transform_fact_orders_summary(fact_orders: pd.DataFrame) -> pd.DataFrame:
    """Aggregate fact_orders into a regional summary."""
    logger.info("Transforming fact_orders_summary...")

    summary = (
        fact_orders.groupby("region", observed=True)
        .agg(
            total_revenue=("line_total", "sum"),
            total_orders=("order_id", "nunique"),
        )
        .reset_index()
    )
    summary["avg_order_value"] = (
        summary["total_revenue"] / summary["total_orders"]
    ).round(2)
    summary["loaded_at"] = datetime.now()

    logger.info(f"  ✓ fact_orders_summary: {len(summary)} region summaries ready")
    return summary[["region", "total_revenue", "total_orders", "avg_order_value", "loaded_at"]]


def transform_all(data: dict) -> dict:
    """Run all transformations and return star schema DataFrames."""
    logger.info("=" * 60)
    logger.info("TRANSFORM PHASE")
    logger.info("=" * 60)

    dim_customers = transform_dim_customers(data["customers"])
    dim_products = transform_dim_products(data["products"])
    fact_orders = transform_fact_orders(data["orders"], data["order_items"], data["products"])
    fact_summary = transform_fact_orders_summary(fact_orders)

    return {
        "dim_customers": dim_customers,
        "dim_products": dim_products,
        "fact_orders": fact_orders,
        "fact_orders_summary": fact_summary,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  LOAD PHASE
# ═══════════════════════════════════════════════════════════════════════════════

def load_to_csv(tables: dict, output_dir: str, partition_by_region: bool = False):
    """Load all tables to CSV files (mock mode)."""
    logger.info("Loading to CSV (mock mode)...")
    os.makedirs(output_dir, exist_ok=True)

    for table_name, df in tables.items():
        if partition_by_region and "region" in df.columns and table_name.startswith("fact_"):
            # Partition fact tables by region
            for region in df["region"].unique():
                region_df = df[df["region"] == region]
                filepath = os.path.join(output_dir, f"{table_name}_{region.lower()}.csv")
                region_df.to_csv(filepath, index=False)
                logger.info(f"  ✓ {filepath} ({len(region_df)} rows)")
        else:
            filepath = os.path.join(output_dir, f"{table_name}.csv")
            df.to_csv(filepath, index=False)
            logger.info(f"  ✓ {filepath} ({len(df)} rows)")


def load_to_snowflake(tables: dict, max_retries: int = 3):
    """Load all tables to Snowflake using snowflake_config.json credentials."""
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
    except ImportError:
        logger.error(
            "snowflake-connector-python not installed. "
            "Install with: pip install 'snowflake-connector-python[pandas]'"
        )
        sys.exit(1)

    # Load Snowflake credentials
    config_path = "snowflake_config.json"
    if not os.path.exists(config_path):
        logger.error(f"{config_path} not found. Cannot load to Snowflake.")
        sys.exit(1)

    with open(config_path, "r") as f:
        sf_config = json.load(f)

    logger.info(f"Connecting to Snowflake: {sf_config['account']}")

    conn = snowflake.connector.connect(
        user=sf_config["user"],
        password=sf_config["password"],
        account=sf_config["account"],
        database=sf_config["database"],
        schema=sf_config["schema"],
        warehouse=sf_config.get("warehouse"),
        role=sf_config.get("role"),
    )

    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {sf_config['database']}")
    cursor.execute(f"USE SCHEMA {sf_config['schema']}")

    # Load order: dimensions first, then facts
    load_order = ["dim_customers", "dim_products", "fact_orders", "fact_orders_summary"]

    for table_name in load_order:
        if table_name not in tables:
            continue

        df = tables[table_name]

        for attempt in range(max_retries):
            try:
                logger.info(f"Loading {table_name} ({len(df)} rows)...")

                # Truncate for fresh load
                cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")

                # Prepare DataFrame for Snowflake (uppercase column names)
                sf_df = df.copy()
                sf_df.columns = [c.upper() for c in sf_df.columns]

                # Use write_pandas for efficient bulk loading
                success, num_chunks, num_rows, _ = write_pandas(
                    conn, sf_df, table_name.upper(), auto_create_table=False
                )

                # Verify
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                logger.info(f"  ✓ {table_name}: {count} rows loaded successfully")
                break

            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(
                        f"  Load attempt {attempt + 1} for {table_name} failed: {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"  ✗ Failed to load {table_name} after {max_retries} attempts: {e}")
                    raise

    cursor.close()
    conn.close()
    logger.info("All tables loaded to Snowflake successfully!")


def load_all(tables: dict, config: dict, use_snowflake: bool = False, partition: bool = False):
    """Load all tables to the target destination."""
    logger.info("=" * 60)
    logger.info("LOAD PHASE")
    logger.info("=" * 60)

    if use_snowflake:
        load_to_snowflake(tables)
    else:
        output_dir = config.get("output_dir", "output/")
        load_to_csv(tables, output_dir, partition_by_region=partition)


# ═══════════════════════════════════════════════════════════════════════════════
#  PIPELINE ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

def run_pipeline(
    config_file: str = "config.yaml",
    use_snowflake: bool = False,
    partition: bool = False,
):
    """Run the full ETL pipeline."""
    start_time = time.time()

    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║         ETL Pipeline - Multi-Source Order Analytics      ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")

    config = load_config(config_file)

    # Extract
    raw_data = extract_all(config)

    # Validate
    validated_data = validate_all(raw_data, config)

    # Transform
    star_schema = transform_all(validated_data)

    # Load
    load_all(star_schema, config, use_snowflake=use_snowflake, partition=partition)

    # Summary
    elapsed = round(time.time() - start_time, 2)
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    for name, df in star_schema.items():
        logger.info(f"  {name}: {len(df)} rows")
    logger.info(f"  Total elapsed time: {elapsed}s")
    logger.info("ETL pipeline completed successfully! ✓")


# ─── CLI ─────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="ETL Pipeline - Multi-Source Order Analytics"
    )
    parser.add_argument(
        "--config", type=str, default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    parser.add_argument(
        "--snowflake", action="store_true",
        help="Load data to Snowflake (requires snowflake_config.json)"
    )
    parser.add_argument(
        "--partition", action="store_true",
        help="Partition fact table output by region"
    )
    parser.add_argument(
        "--log-level", type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override logging level"
    )

    args = parser.parse_args()

    if args.log_level:
        logging.getLogger().setLevel(getattr(logging, args.log_level))

    run_pipeline(
        config_file=args.config,
        use_snowflake=args.snowflake,
        partition=args.partition,
    )


if __name__ == "__main__":
    main()
