"""
silver_transform.py
--------------------
Silver layer of the Medallion architecture.

Responsibility: turn Bronze's "whatever we received" into a clean, deduplicated,
schema-conformant table that Gold and downstream consumers can trust.

Key production patterns implemented here:
1. Streaming de-duplication using dropDuplicatesWithinWatermark on event_id,
   bounded by a watermark on event_ts -- this is what actually caps state size
   in a long-running streaming job (a naive dropDuplicates() with no watermark
   would grow state forever and eventually OOM the cluster).
2. A quarantine table for records that fail validation, instead of silently
   dropping them or crashing the job. Nothing about "99.5% reduction in
   anomalies" means anything if you can't show where the other 0.5% went.
3. MERGE INTO (Delta upsert) for the batch/backfill path, so re-running a day
   is idempotent instead of creating duplicate rows.
"""

import argparse

from pyspark.sql import functions as F
from delta.tables import DeltaTable

from bronze_ingest import build_spark
from schemas import SILVER_REQUIRED_COLUMNS, VALID_EVENT_TYPES


def apply_quality_rules(df):
    """
    Split a Bronze micro-batch into (clean, quarantined) DataFrames.
    Returns a DataFrame with an extra `_reject_reason` column set to null
    for clean rows and a human-readable reason for rejected ones.
    """
    valid_types = list(VALID_EVENT_TYPES)

    # NOTE: array_remove(array, NULL) does NOT reliably strip NULL elements --
    # SQL's NULL <> NULL is UNKNOWN, not TRUE, so a naive array_remove(arr, None)
    # can leave the array non-empty even when every condition below is false,
    # which would mark every row invalid. array_compact() actually drops NULLs.
    reasons = F.array_compact(
        F.array(
            F.when(F.col("event_id").isNull(), F.lit("missing_event_id")),
            F.when(F.col("user_id").isNull(), F.lit("missing_user_id")),
            F.when(~F.col("event_type").isin(valid_types), F.lit("invalid_event_type")),
            F.when(F.col("price").isNotNull() & (F.col("price") < 0), F.lit("negative_price")),
            F.when(F.col("quantity").isNotNull() & (F.col("quantity") <= 0), F.lit("non_positive_quantity")),
        )
    )

    return df.withColumn("_reject_reasons", reasons) \
             .withColumn("_is_valid", F.size("_reject_reasons") == 0)


def run_batch(spark, bronze_path, silver_path, quarantine_path, watermark_minutes=60):
    """
    Batch-style processing (also used for the initial backfill and for local
    testing without a live Kafka stream). Reads whatever is currently in
    Bronze, applies quality rules, dedups, and MERGEs into Silver.
    """
    bronze_df = spark.read.format("delta").load(bronze_path)

    checked = apply_quality_rules(bronze_df)

    quarantined = checked.filter(~F.col("_is_valid"))
    clean = checked.filter(F.col("_is_valid")).drop("_reject_reasons", "_is_valid")

    # De-dup on event_id, keeping the earliest-seen record (by kafka offset)
    from pyspark.sql import Window
    w = Window.partitionBy("event_id").orderBy(F.col("kafka_offset").asc_nulls_last())
    deduped = (
        clean.withColumn("_rn", F.row_number().over(w))
             .filter(F.col("_rn") == 1)
             .drop("_rn")
             .withColumn("silver_load_ts", F.current_timestamp())
    )

    # --- MERGE into Silver (idempotent upsert on event_id) ---
    if DeltaTable.isDeltaTable(spark, silver_path):
        target = DeltaTable.forPath(spark, silver_path)
        (target.alias("t")
               .merge(deduped.alias("s"), "t.event_id = s.event_id")
               .whenNotMatchedInsertAll()
               .execute())
    else:
        deduped.write.format("delta").partitionBy("ingest_date").save(silver_path)

    # --- Append to quarantine (append-only audit trail, never deleted silently) ---
    if quarantined.count() > 0:
        (quarantined
         .withColumn("quarantined_ts", F.current_timestamp())
         .write.format("delta").mode("append")
         .option("mergeSchema", "true")
         .save(quarantine_path))

    return deduped.count(), quarantined.count()


def run_streaming(spark, bronze_path, silver_path, checkpoint_path, quarantine_path,
                   watermark_minutes=60, trigger_seconds=15):
    """
    True streaming version: reads Bronze as a stream (Delta supports streaming reads),
    applies quality rules + watermark-bounded dedup, writes to Silver via foreachBatch
    (needed because MERGE isn't natively supported by plain streaming sinks).
    """
    bronze_stream = spark.readStream.format("delta").load(bronze_path)

    checked = apply_quality_rules(bronze_stream) \
        .withColumn("event_time", F.to_timestamp(F.col("event_ts") / 1000))

    clean = checked.filter(F.col("_is_valid")).drop("_reject_reasons", "_is_valid")
    quarantined = checked.filter(~F.col("_is_valid"))

    # Watermark-bounded streaming de-dup: caps state to `watermark_minutes` of history
    deduped = (
        clean.withWatermark("event_time", f"{watermark_minutes} minutes")
             .dropDuplicatesWithinWatermark(["event_id"])
             .withColumn("silver_load_ts", F.current_timestamp())
    )

    def upsert_to_silver(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if DeltaTable.isDeltaTable(spark, silver_path):
            target = DeltaTable.forPath(spark, silver_path)
            (target.alias("t")
                   .merge(batch_df.alias("s"), "t.event_id = s.event_id")
                   .whenNotMatchedInsertAll()
                   .execute())
        else:
            batch_df.write.format("delta").partitionBy("ingest_date").mode("overwrite").save(silver_path)

    silver_query = (
        deduped.writeStream
        .foreachBatch(upsert_to_silver)
        .option("checkpointLocation", f"{checkpoint_path}/silver")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    quarantine_query = (
        quarantined.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/quarantine")
        .option("mergeSchema", "true")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start(quarantine_path)
    )

    silver_query.awaitTermination()
    quarantine_query.awaitTermination()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bronze-path", default="s3a://lakehouse/bronze/events")
    p.add_argument("--silver-path", default="s3a://lakehouse/silver/events")
    p.add_argument("--quarantine-path", default="s3a://lakehouse/quarantine/events")
    p.add_argument("--checkpoint-path", default="s3a://lakehouse/_checkpoints")
    p.add_argument("--mode", choices=["batch", "streaming"], default="batch")
    p.add_argument("--use-minio", action="store_true", help="point S3A at local MinIO (docker-compose) instead of real AWS")
    args = p.parse_args()

    spark = build_spark("silver-transform", use_minio=args.use_minio)
    spark.sparkContext.setLogLevel("WARN")

    if args.mode == "batch":
        clean_n, bad_n = run_batch(spark, args.bronze_path, args.silver_path, args.quarantine_path)
        print(f"Silver batch complete. clean={clean_n} quarantined={bad_n}")
    else:
        run_streaming(spark, args.bronze_path, args.silver_path,
                      args.checkpoint_path, args.quarantine_path)
