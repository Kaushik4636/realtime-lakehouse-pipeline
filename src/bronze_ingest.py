"""
bronze_ingest.py
-----------------
Bronze layer of the Medallion architecture.

Responsibility: get data from Kafka onto durable, ACID storage (Delta Lake on S3)
as fast and as faithfully as possible. NO business logic, NO filtering here --
Bronze is the immutable system-of-record for "what did we actually receive".

A few design choices worth noting:
- checkpointLocation gives us exactly-once *write* semantics per micro-batch,
  even though Kafka delivery itself is at-least-once. Dedup happens in Silver.
- We store the raw Kafka value as a string column too (event_raw) alongside the
  parsed columns, so a schema-parsing bug never loses data -- we can always
  replay/reparse from the raw payload.
- trigger(processingTime=...) is used instead of default micro-batching so the
  job has a predictable, tunable latency budget instead of running flat-out.
"""

import argparse

from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip

from schemas import RAW_EVENT_SCHEMA


def build_spark(app_name="bronze-ingest", use_minio=False):
    """
    use_minio=True points Spark's S3A connector at a local MinIO instance
    (matches the docker-compose.yml service) instead of real AWS S3, so the
    same s3a:// paths work identically in local dev. Set the env vars
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY to minioadmin/minioadmin (the
    docker-compose default credentials) before running with this flag.

    On Databricks this whole function is unnecessary -- Delta and S3 access
    are natively available on the runtime, no configure_spark_with_delta_pip
    or S3A endpoint override needed. This build_spark() is specifically for
    local/open-source Spark.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")  # small for local/dev; tune per cluster
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    )

    if use_minio:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def run(bootstrap_servers, topic, bronze_path, checkpoint_path, trigger_seconds=10, once=False, use_minio=False):
    spark = build_spark(use_minio=use_minio)
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .withColumn("event_raw", F.col("value").cast("string"))
        .withColumn("parsed", F.from_json(F.col("event_raw"), RAW_EVENT_SCHEMA))
        .select(
            "parsed.*",
            "event_raw",
            F.col("timestamp").alias("kafka_ingest_ts"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .withColumn("bronze_load_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("bronze_load_ts"))  # partition column
    )

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("ingest_date")
        .trigger(processingTime=f"{trigger_seconds} seconds")
    )

    if once:
        query = parsed.writeStream.format("delta").outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("ingest_date").trigger(availableNow=True)

    stream = query.start(bronze_path)
    print(f"Bronze stream started. Writing to {bronze_path}")
    stream.awaitTermination()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="ecommerce.events")
    p.add_argument("--bronze-path", default="s3a://lakehouse/bronze/events")
    p.add_argument("--checkpoint-path", default="s3a://lakehouse/_checkpoints/bronze_events")
    p.add_argument("--trigger-seconds", type=int, default=10)
    p.add_argument("--once", action="store_true", help="run availableNow=True and exit (batch-style backfill)")
    p.add_argument("--use-minio", action="store_true", help="point S3A at local MinIO (docker-compose) instead of real AWS")
    args = p.parse_args()

    run(args.bootstrap_servers, args.topic, args.bronze_path,
        args.checkpoint_path, args.trigger_seconds, args.once, args.use_minio)
