"""
gold_aggregate.py
------------------
Gold layer of the Medallion architecture.

Responsibility: business-ready, aggregated tables that BI tools / analysts
query directly. Gold tables are small, denormalized, and optimized for read
performance -- nobody should ever run an ad-hoc scan over Silver for a
dashboard.

Two Gold tables are produced:
  1. gold_sales_by_category_daily -- revenue, orders, AOV per category per day
  2. gold_funnel_conversion_daily -- page_view -> add_to_cart -> purchase
     conversion rates per day, the kind of metric a growth/product team
     actually looks at.

Uses complete-batch OVERWRITE per run (typical for daily Gold rebuilds) rather
than streaming, since Gold is usually refreshed on a schedule (e.g. via
Airflow), not continuously.
"""

import argparse

from pyspark.sql import functions as F

from bronze_ingest import build_spark


def build_sales_by_category(silver_df):
    purchases = silver_df.filter(F.col("event_type") == "purchase")

    return (
        purchases
        .withColumn("event_date", F.to_date(F.from_unixtime(F.col("event_ts") / 1000)))
        .groupBy("event_date", "category")
        .agg(
            F.sum(F.col("price") * F.col("quantity")).alias("revenue"),
            F.count("*").alias("orders"),
            F.countDistinct("user_id").alias("distinct_buyers"),
        )
        .withColumn("avg_order_value", F.round(F.col("revenue") / F.col("orders"), 2))
        .orderBy("event_date", "category")
    )


def build_funnel_conversion(silver_df):
    with_date = silver_df.withColumn(
        "event_date", F.to_date(F.from_unixtime(F.col("event_ts") / 1000))
    )

    daily_counts = (
        with_date.groupBy("event_date")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        )
    )

    return (
        daily_counts
        .withColumn(
            "view_to_cart_rate",
            F.round(F.col("add_to_carts") / F.col("page_views"), 4),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.round(F.col("purchases") / F.col("add_to_carts"), 4),
        )
        .withColumn(
            "overall_conversion_rate",
            F.round(F.col("purchases") / F.col("page_views"), 4),
        )
        .orderBy("event_date")
    )


def run(silver_path, gold_sales_path, gold_funnel_path, use_minio=False):
    spark = build_spark("gold-aggregate", use_minio=use_minio)
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.format("delta").load(silver_path)

    sales = build_sales_by_category(silver_df)
    funnel = build_funnel_conversion(silver_df)

    (sales.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true")
          .partitionBy("event_date")
          .save(gold_sales_path))

    (funnel.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true")
           .save(gold_funnel_path))

    print(f"Gold refresh complete: {sales.count()} category-day rows, "
          f"{funnel.count()} funnel-day rows")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--silver-path", default="s3a://lakehouse/silver/events")
    p.add_argument("--gold-sales-path", default="s3a://lakehouse/gold/sales_by_category_daily")
    p.add_argument("--gold-funnel-path", default="s3a://lakehouse/gold/funnel_conversion_daily")
    p.add_argument("--use-minio", action="store_true", help="point S3A at local MinIO (docker-compose) instead of real AWS")
    args = p.parse_args()

    run(args.silver_path, args.gold_sales_path, args.gold_funnel_path, args.use_minio)
