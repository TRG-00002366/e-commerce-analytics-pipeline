import os
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_SERVERS", "kafka: 29092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

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
regions = ["US-East", "US-West", "EU-West", "AP-South"]


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


def stream_events():

    print("Starting Kafka event stream (1 event/sec)...")

    while True:

        event = generate_event()

        producer.send(
            "order_events",
            key=event["order_id"].encode(),
            value=event
        )

        print(f"Sent {event['event_type']} | {event['event_id']}")

        time.sleep(1)


if __name__ == "__main__":
    try:
        stream_events()
    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.flush()
        producer.close()