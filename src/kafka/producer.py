"""
Purpose:
    Simulates real-time Amazon order and payment events and publishes them to Kafka topics.

Responsibilities:
    - Connect to Kafka broker.
    - Generate realistic JSON events for `order_events` and `payment_events`.
    - Randomize customer segment, product, shipping type, payment method, and event type.
    - Produce at least 500 events per topic using Faker library.
    - Ensure events conform to schema defined in schemas/order_events.json and schemas/payment_events.json.

Important Behavior:
    - Supports multiple event types: ORDER_CREATED, ORDER_CANCELLED, ORDER_RETURNED, PAYMENT_COMPLETED, PAYMENT_FAILED.
    - Handles retries for transient Kafka connection errors.
    - Logs all successfully produced events and failures for observability.

Design Notes:
    - Decoupled event generation into `util/event_generator.py`.
    - Producer is idempotent at message level via `event_id`.
    - Designed for extensibility to add new topics or event types.
"""