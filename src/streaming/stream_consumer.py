"""
Purpose:
    Consumes real-time Kafka streams of Amazon order and payment events and writes to Bronze layer.

Responsibilities:
    - Connect to Kafka topics `order_events` and `payment_events`.
    - Deserialize JSON messages into Spark DataFrames using StructType schema enforcement.
    - Apply deduplication (via deduplication.py) and handle malformed records.
    - Write raw events to Bronze layer (Parquet, partitioned by date & event_type).

Important Behavior:
    - Micro-batch trigger interval configurable (default 1 minute).
    - Handles late-arriving events with watermarking.
    - Logs streaming metrics: processed record count, duplicates, bad records.

Design Notes:
    - Modular design to allow streaming transformations or joins with additional topics in future.
    - Works as a foundation for downstream batch ETL.
"""