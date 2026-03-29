"""
Kafka producer for generating and streaming fake e-commerce order events to Kafka topic.
"""

import os
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap-servers", default="localhost:9092")
args, _ = parser.parse_known_args()

fake = Faker()

# Event types for orders
EVENT_TYPES = [
    "ORDER_CREATED",
    "ORDER_CANCELLED",
    "ORDER_RETURNED",
    "PAYMENT_COMPLETED",
    "PAYMENT_FAILED"
]
MIN_EVENTS = 500
total_sent = 0
customer_segments = ["REGULAR", "PRIME", "VIP"]
categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
payment_methods = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "APPLE_PAY"]
shipping_types = ["STANDARD", "EXPRESS", "PRIME_EXPRESS"]
regions = ["US-East", "US-West", "EU-West", "AP-South"]

# Generate a random order event
def generate_event():

    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10, 200), 2)
    discount = round(random.uniform(0, 10), 2)

    return {
        "event_id": f"EVT-{fake.unique.random_int(min=90000, max=99999)}",
        "event_type": random.choice(EVENT_TYPES),
        "order_id": f"ORD-{fake.random_int(min=10000, max=99999)}",
        "customer_id": f"CUST-{fake.random_int(min=100, max=999)}",
        "customer_segment": random.choice(customer_segments),

        "product_id": f"PROD-{fake.random_int(min=10, max=99)}",
        "product_name": fake.word().title(),

        "category": random.choice(categories),
        "quantity": quantity,
        "unit_price": unit_price,
        "discount": discount,

        "payment_method": random.choice(payment_methods),
        "shipping_type": random.choice(shipping_types),

        "region": fake.state_abbr(),

        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# Continuously send events to Kafka
def create_producer(bootstrap_servers: str = "localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        api_version=(3, 5, 0),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=30000,
        retries=5,
    )


def on_success(record_metadata):
    global total_sent
    total_sent += 1
    if total_sent % 100 == 0:
        print(f"✅ Sent {total_sent} records to {record_metadata.topic}")


def on_error(excp):
    print(f"Failed to send record: {excp}")


def send_records(topic: str = "order_events", run_length: int = 30):
    producer = create_producer()
    start_time = time.perf_counter()

    print(f"Sending records to topic '{topic}' for {run_length} seconds...")

    try:
        for _ in range(MIN_EVENTS):
            if time.perf_counter() - start_time > run_length:
                break

            record = generate_event()
            producer.send(topic=topic, value=record).add_callback(
                on_success
            ).add_errback(on_error)

            print(f"Sent: {record}")
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("Stopping producer")
    finally:
        producer.flush()


def main():
    print("Starting Kafka Producer")
    time.sleep(5)  # allow Kafka to be ready
    send_records()
    print(f"Finished. Total records sent: {total_sent}")


if __name__ == "__main__":
    main()
