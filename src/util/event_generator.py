"""
Purpose:
    Generates realistic Amazon order and payment events for Kafka producer.

Responsibilities:
    - Create randomized events conforming to schemas/order_events.json and payment_events.json.
    - Include fields such as customer segment, payment method, shipping type, and discount.
    - Support multiple event types: ORDER_CREATED, ORDER_CANCELLED, ORDER_RETURNED, PAYMENT_COMPLETED, PAYMENT_FAILED.
    - Return Python dictionaries ready for Kafka serialization.

Important Behavior:
    - Ensures event_ids are unique for idempotency.
    - Can generate large volumes of events for testing streaming pipelines.
    - Handles locale-specific Faker data to simulate global Amazon customers.

Design Notes:
    - Separate from Kafka producer to enable unit testing and reusability.
    - Extensible for additional event types or marketplaces.
"""

import uuid
import random
from faker import Faker
from datetime import datetime, timezone
import jsonschema

# Define schemas (based on README.md)
ORDER_SCHEMA = {
    "type": "object",
    "properties": {
        "event_id": {"type": "string"},
        "event_type": {"type": "string", "enum": ["ORDER_CREATED", "ORDER_CANCELLED", "ORDER_RETURNED"]},
        "order_id": {"type": "string"},
        "customer_id": {"type": "string"},
        "customer_segment": {"type": "string"},
        "product_id": {"type": "string"},
        "product_name": {"type": "string"},
        "category": {"type": "string"},
        "quantity": {"type": "integer", "minimum": 1},
        "unit_price": {"type": "number", "minimum": 0.01},
        "discount": {"type": "number"},
        "payment_method": {"type": "string"},
        "shipping_type": {"type": "string"},
        "region": {"type": "string"},
        "timestamp": {"type": "string"}
    },
    "required": ["event_id", "event_type", "order_id", "customer_id", "customer_segment", "product_id", "product_name", "category", "quantity", "unit_price", "discount", "payment_method", "shipping_type", "region", "timestamp"]
}

PAYMENT_SCHEMA = {
    "type": "object",
    "properties": {
        "event_id": {"type": "string"},
        "event_type": {"type": "string", "enum": ["PAYMENT_COMPLETED", "PAYMENT_FAILED"]},
        "order_id": {"type": "string"},
        "customer_id": {"type": "string"},
        "payment_method": {"type": "string"},
        "amount": {"type": "number", "minimum": 0.01},
        "status": {"type": "string"},
        "timestamp": {"type": "string"}
    },
    "required": ["event_id", "event_type", "order_id", "customer_id", "payment_method", "amount", "status", "timestamp"]
}

# Predefined choices for event fields
EVENT_TYPES = ["ORDER_CREATED", "ORDER_CANCELLED", "ORDER_RETURNED", "PAYMENT_COMPLETED", "PAYMENT_FAILED"]
CUSTOMER_SEGMENTS = ["PRIME", "NON-PRIME"]
PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "GIFT_CARD"]
SHIPPING_TYPES = ["STANDARD", "PRIME_EXPRESS", "ONE_DAY", "TWO_DAY"]
PRODUCT_CATEGORIES = ["Electronics", "Books", "Home", "Toys", "Clothing", "Sports"]

def generate_order_event(locale="en_US"):
    fake = Faker(locale)
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES[:3]),
        "order_id": f"ORD-{fake.random_int(10000,99999)}",
        "customer_id": f"CUST-{fake.random_int(1000,9999)}",
        "customer_segment": random.choice(CUSTOMER_SEGMENTS),
        "product_id": f"PROD-{fake.random_int(1,200)}",
        "product_name": fake.word().title(),
        "category": random.choice(PRODUCT_CATEGORIES),
        "quantity": random.randint(1,5),  # Ensures > 0
        "unit_price": round(random.uniform(0.01, 500), 2),  # Ensures > 0
        "discount": round(random.uniform(0, 50), 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "shipping_type": random.choice(SHIPPING_TYPES),
        "region": fake.state(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    jsonschema.validate(instance=event, schema=ORDER_SCHEMA)
    return event

def generate_payment_event(order_id=None, locale="en_US"):
    fake = Faker(locale)
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES[3:]),
        "order_id": order_id if order_id else f"ORD-{fake.random_int(10000,99999)}",
        "customer_id": f"CUST-{fake.random_int(1000,9999)}",
        "payment_method": random.choice(PAYMENT_METHODS),
        "amount": round(random.uniform(0.01, 500), 2),  # Ensures > 0
        "status": random.choice(["COMPLETED", "FAILED"]),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    jsonschema.validate(instance=event, schema=PAYMENT_SCHEMA)
    return event

def generate_batch_events(num_orders=100, num_payments=100, locale="en_US"):
    orders = [generate_order_event(locale) for _ in range(num_orders)]
    payments = [generate_payment_event(order_id=random.choice(orders)["order_id"], locale=locale) for _ in range(num_payments)]
    return orders, payments