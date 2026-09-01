"""
schemas.py
----------
Central schema definitions for the Real-Time Lakehouse Pipeline.

We simulate an e-commerce clickstream: page_view, add_to_cart, purchase events.
Defining an explicit StructType (rather than relying on inferSchema) is a
deliberate production practice: it lets Spark fail fast on malformed
records and lets us evolve the schema in a controlled way at the Silver layer.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, LongType, IntegerType
)

# Schema of the raw JSON payload as it arrives on the Kafka topic "ecommerce.events".
#
# IMPORTANT: every field is nullable=True here, even ones that are logically
# "required" (event_id, user_id, event_ts). This is intentional -- Bronze's
# job is to accept and durably store whatever arrived on the topic, including
# malformed producer output. Rejecting a null at the SCHEMA level in Bronze
# would mean Spark throws and the whole micro-batch fails, taking down good
# records along with the one bad one. Instead, nulls are let through here and
# caught explicitly by apply_quality_rules() in silver_transform.py, which
# routes them to quarantine instead of failing the job. Enforcement belongs
# at the Silver gate, not the Bronze schema.
RAW_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=True),        # UUID, used for de-dup
    StructField("event_type", StringType(), nullable=True),      # page_view | add_to_cart | purchase
    StructField("user_id", StringType(), nullable=True),
    StructField("session_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("device", StringType(), nullable=True),          # mobile | desktop | tablet
    StructField("event_ts", LongType(), nullable=True),          # epoch millis, producer-side timestamp
])

# Columns considered mandatory for a record to be admitted to Silver.
# Anything missing these goes to the quarantine/reject path instead of failing the job.
SILVER_REQUIRED_COLUMNS = ["event_id", "event_type", "user_id", "session_id", "event_ts"]

VALID_EVENT_TYPES = {"page_view", "add_to_cart", "purchase"}
