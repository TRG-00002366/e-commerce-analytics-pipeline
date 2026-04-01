"""
Kafka producer for generating and streaming fake e-commerce order events to Kafka topic.
"""

import os
import json
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

def stream_events():

    fake = Faker()

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_SERVERS", "kafka:29092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Event types for orders
    EVENT_TYPES = [
        "ORDER_CREATED",
        "ORDER_CANCELLED",
        "ORDER_RETURNED",
        "PAYMENT_COMPLETED",
        "PAYMENT_FAILED"
    ]

    customer_segments = ["REGULAR", "PRIME", "VIP"]
    categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
    payment_methods = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "APPLE_PAY"]
    shipping_types = ["STANDARD", "EXPRESS", "PRIME_EXPRESS"]

    # Generate a random order event
    def generate_event():

        quantity = random.randint(1, 99)
        unit_price = round(random.uniform(10, 200), 2)
        discount = round(random.uniform(0, 10), 2)

        return {
            "event_id": f"EVT-{fake.unique.random_int(min=90000, max=9_999_999)}",
            "event_type": random.choice(EVENT_TYPES),
            "order_id": f"ORD-{fake.random_int(min=999, max=9_999_999)}",
            "customer_id": f"CUST-{fake.random_int(min=100, max=9999)}",
            "customer_segment": random.choice(customer_segments),

            "product_id": f"PROD-{fake.random_int(min=10, max=9_999_999)}",
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

    print("Starting Kafka event stream (1 event/sec)...")

    max_events = 20000

    for _ in range(max_events):

        event = generate_event()

        producer.send(
            "order_events",
            key=event["order_id"].encode(),
            value=event
        )

        print(f"Sent {event['event_type']} | {event['event_id']}")
    
    print("Stopping producer...")
    producer.flush()
    producer.close()