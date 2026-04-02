"""
Kafka producer for generating and streaming fake e-commerce order events to Kafka topic.
"""
import os
import json
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

MAX_EVENTS = 200000

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

    weighted_categories = {
        "Electronics": 0.3,
        "Books": 0.2,
        "Clothing": 0.2,
        "Home": 0.15,
        "Sports": 0.15
    }

    weighted_payment_methods = {
        "CREDIT_CARD": 0.65,
        "DEBIT_CARD": 0.05,
        "PAYPAL": 0.10,
        "APPLE_PAY": 0.2
    }

    weighted_customer_segments = {
        "REGULAR": 0.7,
        "PRIME": 0.25,
        "VIP": 0.05
    }

    weighted_shipping_types = {
        "STANDARD": 0.7,
        "EXPRESS": 0.2,
        "PRIME_EXPRESS": 0.1
    }
    
    # Generate a random order event
    def generate_event():

        quantity = random.randint(1, 99)
        unit_price = round(random.uniform(10, 200), 2)
        discount = round(random.uniform(0, 10), 2)
        hours = list(range(24))

        weights = []
        for h in hours:
            if 8 <= h <= 10:
                weights.append(3)   # morning peak
            elif 12 <= h <= 14:
                weights.append(3)   # lunch peak
            elif 19 <= h <= 22:
                weights.append(4)   # evening peak (stronger)
            else:
                weights.append(1)   # off-peak

        hour = random.choices(hours, weights=weights, k=1)[0]

        return {
            "event_id": f"EVT-{fake.unique.random_int(min=90000, max=9_999_999)}",
            "event_type": random.choice(EVENT_TYPES),
            "order_id": f"ORD-{fake.random_int(min=999, max=9_999_999)}",
            "customer_id": f"CUST-{fake.random_int(min=100, max=9999)}",
            "customer_segment": random.choices(customer_segments, weights=[weighted_customer_segments[segment] for segment in customer_segments])[0],

            "product_id": f"PROD-{fake.random_int(min=10, max=9_999_999)}",
            "product_name": fake.word().title(),

            "category": random.choices(categories, weights=[weighted_categories[c] for c in categories])[0],
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": discount,

            "payment_method": random.choices(payment_methods, weights=[weighted_payment_methods[method] for method in payment_methods])[0],
            "shipping_type": random.choices(shipping_types, weights=[weighted_shipping_types[stype] for stype in shipping_types])[0],

            "region": fake.state_abbr(),

            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "hour": hour
        }

    print("Starting Kafka event stream...")

    for _ in range(MAX_EVENTS):

        event = generate_event()

        producer.send(
            "order_events",
            key=event["order_id"].encode(),
            value=event
        )

        print(event)
    
    print("Stopping producer...")
    producer.flush()
    producer.close()