import pytest
import pandas as pd
import sys
import os

# Add the current directory to the path to import etl module
sys.path.insert(0, os.path.dirname(__file__))

from etl import transform, validate

def test_transform_null_handling():
    """Test that null amounts are replaced with 0"""
    df = pd.DataFrame([
        {"order_id": 1, "customer_id": 101, "amount": None, "region": "US"},
        {"order_id": 2, "customer_id": 102, "amount": 100, "region": "EU"}
    ])
    result = transform(df)
    assert result["amount"].isnull().sum() == 0

def test_transform_aggregation():
    """Test that data is aggregated by region"""
    df = pd.DataFrame([
        {"order_id": 1, "customer_id": 101, "amount": 100, "region": "US"},
        {"order_id": 2, "customer_id": 102, "amount": 200, "region": "US"},
        {"order_id": 3, "customer_id": 103, "amount": 150, "region": "EU"}
    ])
    result = transform(df)
    assert len(result) == 2  # Two regions
    assert result[result["region"] == "US"]["amount"].values[0] == 300
    assert result[result["region"] == "EU"]["amount"].values[0] == 150

def test_validate_required_columns():
    """Test that missing required columns raise ValueError"""
    df = pd.DataFrame([
        {"order_id": 1, "customer_id": 101, "amount": 100}
    ])
    with pytest.raises(ValueError, match="Missing column"):
        validate(df)

def test_validate_duplicate_order_id():
    """Test that duplicate order_ids are removed"""
    df = pd.DataFrame([
        {"order_id": 1, "customer_id": 101, "amount": 100, "region": "US"},
        {"order_id": 1, "customer_id": 102, "amount": 200, "region": "US"}
    ])
    result = validate(df)
    assert len(result) == 1  # Only one record should remain
    assert result["order_id"].values[0] == 1

def test_validate_negative_amounts():
    """Test that negative amounts are removed"""
    df = pd.DataFrame([
        {"order_id": 1, "customer_id": 101, "amount": 100, "region": "US"},
        {"order_id": 2, "customer_id": 102, "amount": -50, "region": "EU"}
    ])
    result = validate(df)
    assert len(result) == 1  # Only positive amount should remain
    assert result["amount"].values[0] == 100

def test_validate_invalid_regions():
    """Test that invalid regions are filtered out"""
    df = pd.DataFrame([
        {"order_id": 1, "customer_id": 101, "amount": 100, "region": "US"},
        {"order_id": 2, "customer_id": 102, "amount": 200, "region": "INVALID"}
    ])
    result = validate(df)
    assert len(result) == 1  # Only valid region should remain
    assert result["region"].values[0] == "US"
