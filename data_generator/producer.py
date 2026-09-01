"""
producer.py
-----------
Simulates a real e-commerce clickstream and publishes events to a Kafka topic.

Deliberately injects a small percentage of "bad" traffic (duplicates, nulls,
out-of-range prices, unknown event types) so the Bronze -> Silver quality
gate downstream has something real to catch. A pipeline that only ever
sees clean data doesn't prove anything.

Usage:
    python producer.py --bootstrap-servers localhost:9092 --topic ecommerce.events --rate 50
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [0.7, 0.22, 0.08]          # funnel shape: most traffic is just browsing
CATEGORIES = ["electronics", "apparel", "home", "beauty", "sports", "books"]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["IN", "US", "GB", "DE", "AE", "SG"]


def make_event(session_pool):
    """Build one synthetic event, occasionally corrupting it to mimic real-world noise."""
    user_id = f"u_{random.randint(1, 5000)}"
    session_id = session_pool.setdefault(user_id, f"s_{uuid.uuid4().hex[:12]}")

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0],
        "user_id": user_id,
        "session_id": session_id,
        "product_id": f"p_{random.randint(1000, 1999)}",
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(5, 500), 2),
        "quantity": random.randint(1, 3),
        "country": random.choice(COUNTRIES),
        "device": random.choice(DEVICES),
        "event_ts": int(datetime.now(timezone.utc).timestamp() * 1000),
    }

    # ~3% of events are deliberately corrupted -- this is what Great Expectations
    # and the Silver quarantine path are supposed to catch.
    roll = random.random()
    if roll < 0.01:
        event["event_type"] = "unknown_action"          # invalid enum value
    elif roll < 0.02:
        event["price"] = -event["price"]                # negative price, impossible
    elif roll < 0.03:
        event["user_id"] = None                          # missing required field

    return event


def duplicate_burst(event, n=1):
    """Occasionally resend the exact same event_id to simulate at-least-once delivery."""
    return [event] * n


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="ecommerce.events")
    parser.add_argument("--rate", type=int, default=20, help="events per second")
    parser.add_argument("--duration", type=int, default=0, help="seconds to run, 0 = forever")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=20,
        acks="all",             # don't drop events on the way in -- matches ingestion guarantee we advertise
    )

    session_pool = {}
    sent = 0
    start = time.time()

    print(f"Producing to topic='{args.topic}' at ~{args.rate} events/sec. Ctrl+C to stop.")
    try:
        while True:
            event = make_event(session_pool)
            # ~2% chance of a duplicate burst (simulates Kafka at-least-once redelivery)
            batch = duplicate_burst(event, n=2) if random.random() < 0.02 else [event]

            for e in batch:
                producer.send(args.topic, key=e["user_id"] or "unknown", value=e)
                sent += 1

            if sent % 500 == 0:
                print(f"  ...sent {sent} events")

            if args.duration and (time.time() - start) > args.duration:
                break

            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print(f"Done. Total events sent: {sent}")


if __name__ == "__main__":
    main()
