"""
test_transformations.py
------------------------
Pytest unit tests for the pure transformation logic (quality rules and Gold
aggregations). These run fast, on tiny in-memory DataFrames, and are what
would run in CI on every PR -- distinct from test_logic_smoke.py, which is
a slower end-to-end volume test meant to be run manually/locally.

Run: pytest tests/test_transformations.py -v
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from silver_transform import apply_quality_rules  # noqa: E402
from gold_aggregate import build_sales_by_category, build_funnel_conversion  # noqa: E402

# Explicit schema so rows with an all-NULL column (e.g. event_id=None) don't
# break Spark's type inference, which can't determine a type from an
# all-null single-row sample.
QUALITY_TEST_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
])


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .appName("pytest-transformations")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_valid_row_passes_quality_rules(spark):
    df = spark.createDataFrame([
        {"event_id": "e1", "event_type": "purchase", "user_id": "u1",
         "price": 19.99, "quantity": 2},
    ])
    result = apply_quality_rules(df).collect()[0]
    assert result["_is_valid"] is True
    assert result["_reject_reasons"] == []


def test_missing_event_id_is_quarantined(spark):
    df = spark.createDataFrame([
        {"event_id": None, "event_type": "purchase", "user_id": "u1",
         "price": 19.99, "quantity": 1},
    ], schema=QUALITY_TEST_SCHEMA)
    result = apply_quality_rules(df).collect()[0]
    assert result["_is_valid"] is False
    assert "missing_event_id" in result["_reject_reasons"]


def test_negative_price_is_quarantined(spark):
    df = spark.createDataFrame([
        {"event_id": "e2", "event_type": "purchase", "user_id": "u1",
         "price": -10.0, "quantity": 1},
    ])
    result = apply_quality_rules(df).collect()[0]
    assert result["_is_valid"] is False
    assert "negative_price" in result["_reject_reasons"]


def test_invalid_event_type_is_quarantined(spark):
    df = spark.createDataFrame([
        {"event_id": "e3", "event_type": "not_a_real_event", "user_id": "u1",
         "price": 5.0, "quantity": 1},
    ])
    result = apply_quality_rules(df).collect()[0]
    assert result["_is_valid"] is False
    assert "invalid_event_type" in result["_reject_reasons"]


def test_multiple_violations_are_all_captured(spark):
    df = spark.createDataFrame([
        {"event_id": None, "event_type": "bogus", "user_id": None,
         "price": -5.0, "quantity": 0},
    ], schema=QUALITY_TEST_SCHEMA)
    result = apply_quality_rules(df).collect()[0]
    reasons = set(result["_reject_reasons"])
    assert reasons == {
        "missing_event_id", "missing_user_id", "invalid_event_type",
        "negative_price", "non_positive_quantity",
    }


def test_gold_sales_by_category_computes_correct_revenue(spark):
    df = spark.createDataFrame([
        {"event_type": "purchase", "category": "books", "price": 10.0,
         "quantity": 2, "user_id": "u1", "event_ts": 1735689600000},
        {"event_type": "purchase", "category": "books", "price": 5.0,
         "quantity": 1, "user_id": "u2", "event_ts": 1735689600000},
        {"event_type": "page_view", "category": "books", "price": None,
         "quantity": None, "user_id": "u3", "event_ts": 1735689600000},
    ])
    result = build_sales_by_category(df).collect()
    assert len(result) == 1
    row = result[0]
    assert row["category"] == "books"
    assert row["revenue"] == 25.0  # (10*2) + (5*1), page_view excluded
    assert row["orders"] == 2
    assert row["distinct_buyers"] == 2


def test_gold_funnel_conversion_rates(spark):
    rows = (
        [{"event_type": "page_view", "user_id": f"u{i}", "event_ts": 1735689600000} for i in range(100)]
        + [{"event_type": "add_to_cart", "user_id": f"u{i}", "event_ts": 1735689600000} for i in range(20)]
        + [{"event_type": "purchase", "user_id": f"u{i}", "event_ts": 1735689600000} for i in range(5)]
    )
    df = spark.createDataFrame(rows)
    result = build_funnel_conversion(df).collect()[0]
    assert result["page_views"] == 100
    assert result["add_to_carts"] == 20
    assert result["purchases"] == 5
    assert result["view_to_cart_rate"] == pytest.approx(0.20, abs=0.001)
    assert result["cart_to_purchase_rate"] == pytest.approx(0.25, abs=0.001)
    assert result["overall_conversion_rate"] == pytest.approx(0.05, abs=0.001)
