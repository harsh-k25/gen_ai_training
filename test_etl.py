"""
Tests for the ETL Pipeline
===========================
Tests extract, validate, transform phases using the sample data files.
"""

import pandas as pd
import pytest
import os
import shutil
from datetime import datetime

from etl import (
    load_config,
    extract_orders,
    extract_customers,
    extract_products,
    extract_order_items,
    extract_all,
    validate_customers,
    validate_products,
    validate_orders,
    validate_order_items,
    transform_dim_customers,
    transform_dim_products,
    transform_fact_orders,
    transform_fact_orders_summary,
    load_to_csv,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────────

@pytest.fixture
def config():
    return load_config("config.yaml")


@pytest.fixture
def raw_data(config):
    return extract_all(config)


@pytest.fixture
def validated_data(raw_data, config):
    valid_regions = config.get("valid_regions", ["US", "EU", "APAC"])
    return {
        "customers": validate_customers(raw_data["customers"]),
        "products": validate_products(raw_data["products"]),
        "orders": validate_orders(raw_data["orders"], valid_regions),
        "order_items": validate_order_items(raw_data["order_items"]),
    }


@pytest.fixture
def output_dir():
    """Create and cleanup a test output directory."""
    test_dir = "test_output/"
    os.makedirs(test_dir, exist_ok=True)
    yield test_dir
    shutil.rmtree(test_dir, ignore_errors=True)


# ─── Extract Tests ───────────────────────────────────────────────────────────────

class TestExtract:
    def test_extract_orders(self, config):
        df = extract_orders(config)
        assert len(df) > 0
        assert "order_id" in df.columns
        assert "customer_id" in df.columns
        assert "amount" in df.columns
        assert "region" in df.columns

    def test_extract_customers(self, config):
        df = extract_customers(config)
        assert len(df) > 0
        assert "customer_id" in df.columns
        assert "customer_name" in df.columns
        assert "email" in df.columns

    def test_extract_products(self, config):
        df = extract_products(config)
        assert len(df) > 0
        assert "product_id" in df.columns
        assert "product_name" in df.columns
        assert "unit_price" in df.columns

    def test_extract_order_items(self, config):
        df = extract_order_items(config)
        assert len(df) > 0
        assert "order_item_id" in df.columns
        assert "order_id" in df.columns
        assert "product_id" in df.columns
        assert "quantity" in df.columns

    def test_extract_all_returns_all_sources(self, raw_data):
        assert "orders" in raw_data
        assert "customers" in raw_data
        assert "products" in raw_data
        assert "order_items" in raw_data
        for key, df in raw_data.items():
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0


# ─── Validate Tests ─────────────────────────────────────────────────────────────

class TestValidate:
    def test_validate_customers_removes_duplicates(self):
        df = pd.DataFrame({
            "customer_id": [1, 1, 2],
            "customer_name": ["Alice", "Alice Dup", "Bob"],
            "email": ["a@a.com", "a2@a.com", "b@b.com"],
            "region": ["US", "US", "EU"],
            "tier": ["Gold", "Silver", "Bronze"],
        })
        result = validate_customers(df)
        assert len(result) == 2
        assert result.iloc[0]["customer_name"] == "Alice"

    def test_validate_customers_normalizes_email(self):
        df = pd.DataFrame({
            "customer_id": [1],
            "customer_name": ["Alice"],
            "email": ["  ALICE@Example.COM  "],
            "region": ["US"],
            "tier": ["Gold"],
        })
        result = validate_customers(df)
        assert result.iloc[0]["email"] == "alice@example.com"

    def test_validate_customers_fixes_invalid_tier(self):
        df = pd.DataFrame({
            "customer_id": [1],
            "customer_name": ["Alice"],
            "email": ["a@a.com"],
            "region": ["US"],
            "tier": ["INVALID_TIER"],
        })
        result = validate_customers(df)
        assert result.iloc[0]["tier"] == "Bronze"

    def test_validate_products_removes_negative_prices(self):
        df = pd.DataFrame({
            "product_id": ["P001", "P002"],
            "product_name": ["Laptop", "Free Item"],
            "category": ["Electronics", "Other"],
            "unit_price": [1200.0, -50.0],
        })
        result = validate_products(df)
        assert result[result["product_id"] == "P002"]["unit_price"].iloc[0] == 0

    def test_validate_orders_filters_invalid_regions(self):
        df = pd.DataFrame({
            "order_id": [1, 2],
            "customer_id": [101, 102],
            "amount": [100, 200],
            "region": ["US", "INVALID"],
        })
        result = validate_orders(df, ["US", "EU", "APAC"])
        assert len(result) == 1
        assert result.iloc[0]["region"] == "US"

    def test_validate_orders_removes_negative_amounts(self):
        df = pd.DataFrame({
            "order_id": [1, 2],
            "customer_id": [101, 102],
            "amount": [100.0, -50.0],
            "region": ["US", "US"],
        })
        result = validate_orders(df, ["US"])
        assert len(result) == 1

    def test_validate_orders_fills_null_amounts(self):
        df = pd.DataFrame({
            "order_id": [1],
            "customer_id": [101],
            "amount": [None],
            "region": ["US"],
        })
        result = validate_orders(df, ["US"])
        assert result.iloc[0]["amount"] == 0

    def test_validate_order_items_removes_zero_quantity(self):
        df = pd.DataFrame({
            "order_item_id": [1, 2],
            "order_id": [1, 1],
            "product_id": ["P001", "P002"],
            "quantity": [2, 0],
            "discount_pct": [0.0, 0.0],
        })
        result = validate_order_items(df)
        assert len(result) == 1

    def test_validate_order_items_clamps_discount(self):
        df = pd.DataFrame({
            "order_item_id": [1, 2],
            "order_id": [1, 1],
            "product_id": ["P001", "P002"],
            "quantity": [1, 1],
            "discount_pct": [-0.5, 1.5],
        })
        result = validate_order_items(df)
        assert result.iloc[0]["discount_pct"] == 0
        assert result.iloc[1]["discount_pct"] == 1


# ─── Transform Tests ────────────────────────────────────────────────────────────

class TestTransform:
    def test_dim_customers_has_loaded_at(self, validated_data):
        result = transform_dim_customers(validated_data["customers"])
        assert "loaded_at" in result.columns
        assert result["loaded_at"].notna().all()

    def test_dim_customers_columns(self, validated_data):
        result = transform_dim_customers(validated_data["customers"])
        expected_cols = {"customer_id", "customer_name", "email", "region", "tier", "loaded_at"}
        assert set(result.columns) == expected_cols

    def test_dim_products_has_loaded_at(self, validated_data):
        result = transform_dim_products(validated_data["products"])
        assert "loaded_at" in result.columns

    def test_dim_products_columns(self, validated_data):
        result = transform_dim_products(validated_data["products"])
        expected_cols = {"product_id", "product_name", "category", "unit_price", "loaded_at"}
        assert set(result.columns) == expected_cols

    def test_fact_orders_line_total(self, validated_data):
        fact = transform_fact_orders(
            validated_data["orders"],
            validated_data["order_items"],
            validated_data["products"],
        )
        assert "line_total" in fact.columns
        # All line totals should be non-negative
        assert (fact["line_total"] >= 0).all()

    def test_fact_orders_columns(self, validated_data):
        fact = transform_fact_orders(
            validated_data["orders"],
            validated_data["order_items"],
            validated_data["products"],
        )
        expected_cols = {
            "order_item_id", "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "discount_pct", "line_total",
            "region", "loaded_at",
        }
        assert set(fact.columns) == expected_cols

    def test_fact_orders_line_total_calculation(self):
        """Verify: line_total = quantity × unit_price × (1 - discount_pct)"""
        orders = pd.DataFrame({
            "order_id": [1],
            "customer_id": [101],
            "amount": [100],
            "region": ["US"],
        })
        order_items = pd.DataFrame({
            "order_item_id": [1],
            "order_id": [1],
            "product_id": ["P001"],
            "quantity": [2],
            "discount_pct": [0.1],
        })
        products = pd.DataFrame({
            "product_id": ["P001"],
            "product_name": ["Laptop"],
            "category": ["Electronics"],
            "unit_price": [1000.0],
        })
        fact = transform_fact_orders(orders, order_items, products)
        # 2 × 1000 × (1 - 0.1) = 1800
        assert fact.iloc[0]["line_total"] == 1800.0

    def test_fact_orders_summary(self, validated_data):
        fact = transform_fact_orders(
            validated_data["orders"],
            validated_data["order_items"],
            validated_data["products"],
        )
        summary = transform_fact_orders_summary(fact)
        assert "total_revenue" in summary.columns
        assert "total_orders" in summary.columns
        assert "avg_order_value" in summary.columns
        assert len(summary) > 0

    def test_fact_orders_summary_math(self):
        """Verify summary aggregation is correct."""
        fact = pd.DataFrame({
            "order_item_id": [1, 2, 3],
            "order_id": [1, 1, 2],
            "customer_id": [101, 101, 102],
            "product_id": ["P001", "P002", "P001"],
            "quantity": [1, 2, 1],
            "unit_price": [100, 50, 100],
            "discount_pct": [0, 0, 0],
            "line_total": [100.0, 100.0, 100.0],
            "region": ["US", "US", "US"],
            "loaded_at": [datetime.now()] * 3,
        })
        summary = transform_fact_orders_summary(fact)
        assert len(summary) == 1
        assert summary.iloc[0]["total_revenue"] == 300.0
        assert summary.iloc[0]["total_orders"] == 2
        assert summary.iloc[0]["avg_order_value"] == 150.0


# ─── Load Tests ──────────────────────────────────────────────────────────────────

class TestLoad:
    def test_load_to_csv_creates_files(self, validated_data, output_dir):
        tables = {
            "dim_customers": transform_dim_customers(validated_data["customers"]),
            "dim_products": transform_dim_products(validated_data["products"]),
        }
        load_to_csv(tables, output_dir)
        assert os.path.exists(os.path.join(output_dir, "dim_customers.csv"))
        assert os.path.exists(os.path.join(output_dir, "dim_products.csv"))

    def test_load_to_csv_partitioned(self, validated_data, output_dir):
        fact = transform_fact_orders(
            validated_data["orders"],
            validated_data["order_items"],
            validated_data["products"],
        )
        tables = {"fact_orders": fact}
        load_to_csv(tables, output_dir, partition_by_region=True)
        # Should create region-specific files
        files = os.listdir(output_dir)
        partitioned = [f for f in files if f.startswith("fact_orders_")]
        assert len(partitioned) > 0

    def test_load_csv_row_counts(self, validated_data, output_dir):
        dim_cust = transform_dim_customers(validated_data["customers"])
        tables = {"dim_customers": dim_cust}
        load_to_csv(tables, output_dir)
        loaded = pd.read_csv(os.path.join(output_dir, "dim_customers.csv"))
        assert len(loaded) == len(dim_cust)
