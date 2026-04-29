"""
Snowflake Schema Setup Script
==============================
Creates the database, schema, and all tables for the ETL pipeline.
Uses snowflake_config.json for credentials.

Usage:
    python snowflake_setup.py                    # Uses snowflake-connector-python
    python snowflake_setup.py --use-snowsql      # Generates SQL file and runs via SnowSQL CLI
    python snowflake_setup.py --sql-only          # Only generates the SQL file (no execution)
"""

import json
import argparse
import logging
import subprocess
import sys
import os
from datetime import datetime


# ─── Logging ────────────────────────────────────────────────────────────────────
class StructuredFormatter(logging.Formatter):
    def format(self, record):
        timestamp = datetime.now().isoformat()
        return f"{timestamp} | {record.levelname} | {record.getMessage()}"


handler = logging.StreamHandler()
handler.setFormatter(StructuredFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)


# ─── SQL Definitions ────────────────────────────────────────────────────────────

def get_setup_sql(database: str, schema: str) -> str:
    """Return the full DDL script to create database, schema, and all tables."""
    return f"""
-- ============================================================
-- ETL Pipeline - Snowflake Schema Setup
-- Generated: {datetime.now().isoformat()}
-- ============================================================

-- Create database (if not exists)
CREATE DATABASE IF NOT EXISTS {database};
USE DATABASE {database};

-- Create schema (if not exists)
CREATE SCHEMA IF NOT EXISTS {schema};
USE SCHEMA {schema};

-- ─── Dimension: Customers ──────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id     INTEGER       PRIMARY KEY,
    customer_name   VARCHAR(200)  NOT NULL,
    email           VARCHAR(300),
    region          VARCHAR(50),
    tier            VARCHAR(50),
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ─── Dimension: Products ───────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_products (
    product_id      VARCHAR(20)   PRIMARY KEY,
    product_name    VARCHAR(200)  NOT NULL,
    category        VARCHAR(100),
    unit_price      FLOAT,
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ─── Fact: Orders (line-item grain) ────────────────────────
CREATE TABLE IF NOT EXISTS fact_orders (
    order_item_id   INTEGER       PRIMARY KEY,
    order_id        INTEGER       NOT NULL,
    customer_id     INTEGER       NOT NULL,
    product_id      VARCHAR(20)   NOT NULL,
    quantity        INTEGER,
    unit_price      FLOAT,
    discount_pct    FLOAT         DEFAULT 0.0,
    line_total      FLOAT,
    region          VARCHAR(50),
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);

-- ─── Fact: Orders Summary (aggregate) ─────────────────────
CREATE TABLE IF NOT EXISTS fact_orders_summary (
    region          VARCHAR(50)   PRIMARY KEY,
    total_revenue   FLOAT,
    total_orders    INTEGER,
    avg_order_value FLOAT,
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- Setup Complete
-- ============================================================
"""


# ─── Connector-based Setup ──────────────────────────────────────────────────────

def setup_via_connector(sf_config: dict):
    """Create schema and tables using snowflake-connector-python."""
    try:
        import snowflake.connector
    except ImportError:
        logger.error(
            "snowflake-connector-python is not installed. "
            "Install it with: pip install snowflake-connector-python"
        )
        sys.exit(1)

    database = sf_config["database"]
    schema = sf_config["schema"]

    logger.info(f"Connecting to Snowflake account: {sf_config['account']}")
    conn = snowflake.connector.connect(
        user=sf_config["user"],
        password=sf_config["password"],
        account=sf_config["account"],
        warehouse=sf_config.get("warehouse"),
        role=sf_config.get("role"),
    )

    cursor = conn.cursor()
    sql_script = get_setup_sql(database, schema)

    # Execute each statement separately
    statements = sql_script.split(";")
    for stmt in statements:
        # Skip comment-only lines and build clean statement
        lines = [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
        if not lines:
            continue
        clean_stmt = "\n".join(lines).strip()
        if not clean_stmt:
            continue
        logger.info(f"Executing: {clean_stmt[:80]}...")
        cursor.execute(clean_stmt)

    # Verify tables created
    cursor.execute(f"USE DATABASE {database}")
    cursor.execute(f"USE SCHEMA {schema}")
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    logger.info(f"Tables in {database}.{schema}:")
    for table in tables:
        # table[1] is the table name in SHOW TABLES output
        logger.info(f"  ✓ {table[1]}")

    cursor.close()
    conn.close()
    logger.info("Snowflake schema setup completed successfully!")


# ─── SnowSQL CLI Setup ──────────────────────────────────────────────────────────

def setup_via_snowsql(sf_config: dict, sql_only: bool = False):
    """Generate SQL file and optionally execute via SnowSQL CLI."""
    database = sf_config["database"]
    schema = sf_config["schema"]

    sql_script = get_setup_sql(database, schema)
    sql_file = "snowflake_setup.sql"

    # Write SQL to file
    with open(sql_file, "w") as f:
        f.write(sql_script)
    logger.info(f"SQL script written to: {sql_file}")

    if sql_only:
        logger.info("SQL-only mode. Skipping execution.")
        print(f"\nTo execute manually:\n  snowsql -c my_connection -f {sql_file}")
        return

    # Execute via SnowSQL CLI
    snowsql_cmd = [
        "snowsql",
        "-a", sf_config["account"],
        "-u", sf_config["user"],
        "-d", database,
        "-s", schema,
        "-w", sf_config.get("warehouse", "COMPUTE_WH"),
        "-r", sf_config.get("role", "SYSADMIN"),
        "-f", sql_file,
        "-o", "exit_on_error=true",
    ]

    logger.info("Executing via SnowSQL CLI...")
    logger.info(f"Command: snowsql -a {sf_config['account']} -u {sf_config['user']} -f {sql_file}")

    try:
        result = subprocess.run(
            snowsql_cmd,
            capture_output=True,
            text=True,
            env={**os.environ, "SNOWSQL_PWD": sf_config["password"]},
        )

        if result.returncode == 0:
            logger.info("SnowSQL execution completed successfully!")
            if result.stdout:
                print(result.stdout)
        else:
            logger.error(f"SnowSQL execution failed (exit code {result.returncode})")
            if result.stderr:
                logger.error(result.stderr)
            sys.exit(1)

    except FileNotFoundError:
        logger.error(
            "SnowSQL CLI not found. Install it from: "
            "https://docs.snowflake.com/en/user-guide/snowsql-install-config"
        )
        logger.info(f"You can still run the SQL manually: snowsql -f {sql_file}")
        sys.exit(1)


# ─── Main ───────────────────────────────────────────────────────────────────────

def load_config(config_path: str = "snowflake_config.json") -> dict:
    """Load Snowflake credentials from JSON config file."""
    if not os.path.exists(config_path):
        logger.error(
            f"{config_path} not found. "
            f"Copy snowflake_config.json.example to {config_path} and fill in your credentials."
        )
        sys.exit(1)

    with open(config_path, "r") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(
        description="Snowflake Schema Setup - Creates database, schema, and tables for the ETL pipeline"
    )
    parser.add_argument(
        "--config", type=str, default="snowflake_config.json",
        help="Path to Snowflake config JSON (default: snowflake_config.json)"
    )
    parser.add_argument(
        "--use-snowsql", action="store_true",
        help="Use SnowSQL CLI instead of snowflake-connector-python"
    )
    parser.add_argument(
        "--sql-only", action="store_true",
        help="Only generate the SQL file without executing (implies --use-snowsql)"
    )

    args = parser.parse_args()
    sf_config = load_config(args.config)

    logger.info("=" * 60)
    logger.info("ETL Pipeline - Snowflake Schema Setup")
    logger.info(f"Database: {sf_config['database']}")
    logger.info(f"Schema:   {sf_config['schema']}")
    logger.info("=" * 60)

    if args.sql_only or args.use_snowsql:
        setup_via_snowsql(sf_config, sql_only=args.sql_only)
    else:
        setup_via_connector(sf_config)


if __name__ == "__main__":
    main()
