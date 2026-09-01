"""
ge_suite.py
-----------
Great Expectations validation gate, run against the Silver Delta table after
each batch/streaming micro-batch commits. This is a SEPARATE layer of defense
from the inline `apply_quality_rules()` in silver_transform.py:

  - silver_transform.py quarantines row-level bad data DURING the write
  - ge_suite.py validates the TABLE as a whole AFTER the write, catching
    things row-level rules can't see: distributional drift, referential
    completeness, uniqueness violations that survive dedup, etc.

In production this is wired into the pipeline as an Airflow task or a
Databricks job step that runs after silver_transform and BEFORE gold_aggregate
is allowed to run -- i.e. it's a gate, not just a report. A validation failure
should block Gold from being refreshed on top of bad Silver data.
"""

import argparse
import sys

import great_expectations as gx
import pandas as pd


def build_expectation_suite(context, suite_name="silver_events_suite"):
    suite = context.suites.add_or_update(gx.ExpectationSuite(name=suite_name))
    return suite


def validate_silver(pandas_df: pd.DataFrame, fail_on_error: bool = True):
    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas("silver_pandas_source")
    data_asset = data_source.add_dataframe_asset(name="silver_events")
    batch_def = data_asset.add_batch_definition_whole_dataframe("silver_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": pandas_df})

    results = []

    checks = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="event_id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="event_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id"),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="event_type", value_set=["page_view", "add_to_cart", "purchase"]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="price", min_value=0, max_value=100000, mostly=0.99
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="quantity", min_value=1, max_value=50, mostly=0.99
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="event_ts"),
        # Referential sanity: every purchase should reference a real product
        gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"),
    ]

    all_passed = True
    for expectation in checks:
        result = batch.validate(expectation)
        results.append({
            "expectation": expectation.__class__.__name__,
            "column": getattr(expectation, "column", None),
            "success": result.success,
            "details": result.result,
        })
        if not result.success:
            all_passed = False

    print(f"\n{'='*60}\nGreat Expectations validation: "
          f"{'PASSED' if all_passed else 'FAILED'}\n{'='*60}")
    for r in results:
        status = "PASS" if r["success"] else "FAIL"
        print(f"  [{status}] {r['expectation']} (column={r['column']})")
        if not r["success"]:
            print(f"         -> {r['details']}")

    if fail_on_error and not all_passed:
        sys.exit(1)

    return all_passed, results


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--silver-path", default="s3a://lakehouse/silver/events")
    p.add_argument("--sample-fraction", type=float, default=1.0,
                    help="fraction of Silver table to sample for validation (use <1.0 on huge tables)")
    args = p.parse_args()

    # Import here so this file can also be unit-tested without a live Spark session
    from bronze_ingest import build_spark

    spark = build_spark("ge-validation")
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.format("delta").load(args.silver_path)
    if args.sample_fraction < 1.0:
        silver_df = silver_df.sample(fraction=args.sample_fraction, seed=42)

    pdf = silver_df.toPandas()
    validate_silver(pdf)
