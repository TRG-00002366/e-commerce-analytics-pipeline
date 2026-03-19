import os
from unittest.mock import MagicMock, patch

import pytest

from src.streaming.stream_consumer import order_schema, main


def test_order_schema_contains_expected_fields():
    expected_fields = [
        "event_id",
        "event_type",
        "order_id",
        "customer_id",
        "customer_segment",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "discount",
        "payment_method",
        "shipping_type",
        "region",
        "timestamp",
    ]

    assert [field.name for field in order_schema.fields] == expected_fields

# Verify that main configures Spark Structured Streaming and starts the query.
def test_main_sets_up_streaming_query_and_awaits_termination():

    spark_mock = MagicMock()
    builder_mock = MagicMock()

    # SparkSession.builder chain
    builder_mock.appName.return_value = builder_mock
    builder_mock.master.return_value = builder_mock
    builder_mock.config.return_value = builder_mock
    builder_mock.getOrCreate.return_value = spark_mock

    # Mock the streaming read pipeline
    read_stream = MagicMock()
    spark_mock.readStream = read_stream
    read_stream.format.return_value = read_stream
    read_stream.option.return_value = read_stream

    df_kafka = MagicMock()
    read_stream.load.return_value = df_kafka

    messages_df = MagicMock()
    df_kafka.selectExpr.return_value = messages_df

    df_events_raw = MagicMock()
    messages_df.select.return_value = df_events_raw

    # Chaining transformations (select/withColumn/filter should return the same mock)
    df_events_raw.select.return_value = df_events_raw
    df_events_raw.withColumn.return_value = df_events_raw
    df_events_raw.filter.return_value = df_events_raw

    # Mock the writeStream chain
    write_stream = MagicMock()
    df_events_raw.writeStream = write_stream
    write_stream.option.return_value = write_stream
    write_stream.trigger.return_value = write_stream
    write_stream.foreachBatch.return_value = write_stream

    query_mock = MagicMock()
    write_stream.start.return_value = query_mock
    query_mock.awaitTermination.return_value = None

    with patch("src.streaming.stream_consumer.SparkSession") as SparkSessionMock, \
         patch(
             "src.streaming.stream_consumer.col",
             side_effect=lambda c: MagicMock(isNotNull=MagicMock(return_value=MagicMock())),
         ), \
         patch(
             "src.streaming.stream_consumer.from_json",
             side_effect=lambda col_obj, schema: MagicMock(alias=lambda name: MagicMock()),
         ), \
         patch("src.streaming.stream_consumer.to_timestamp", side_effect=lambda c: f"to_timestamp({c})"), \
         patch("src.streaming.stream_consumer.to_date", side_effect=lambda c: f"to_date({c})"):
        SparkSessionMock.builder = builder_mock

        env = {
            "SPARK_MASTER_URL": "spark://test:7077",
            "KAFKA_SERVERS": "kafka:1234",
            "BRONZE_PATH": "/tmp/bronze",
            "SILVER_PATH": "/tmp/silver",
            "CHECKPOINT_PATH": "/tmp/check",
            "SPARK_SQL_SHUFFLE_PARTITIONS": "5",
        }
        with patch.dict(os.environ, env, clear=True):
            main()

    # Verify builder configuration calls
    builder_mock.appName.assert_called_once_with("OrderEventStreamConsumer")
    builder_mock.master.assert_called_once_with(env["SPARK_MASTER_URL"])
    builder_mock.config.assert_called_once_with(
        "spark.sql.shuffle.partitions", env["SPARK_SQL_SHUFFLE_PARTITIONS"]
    )

    # Verify Kafka source setup
    read_stream.format.assert_called_once_with("kafka")
    read_stream.option.assert_any_call("kafka.bootstrap.servers", env["KAFKA_SERVERS"])
    read_stream.option.assert_any_call("subscribe", "order_events")
    read_stream.option.assert_any_call("startingOffsets", "earliest")
    read_stream.option.assert_any_call("failOnDataLoss", "false")
    df_kafka.selectExpr.assert_called_once_with("CAST(value AS STRING) as json")

    # Verify checkpoint location was passed to the stream writer
    write_stream.option.assert_any_call("checkpointLocation", env["CHECKPOINT_PATH"])
    query_mock.awaitTermination.assert_called_once()


def test_process_batch_skips_empty_dataframe(monkeypatch):
    """Ensure process_batch does not write anything when the batch is empty."""

    spark_mock = MagicMock()
    builder_mock = MagicMock()

    # SparkSession.builder chain
    builder_mock.appName.return_value = builder_mock
    builder_mock.master.return_value = builder_mock
    builder_mock.config.return_value = builder_mock
    builder_mock.getOrCreate.return_value = spark_mock

    # Mock the streaming read pipeline
    read_stream = MagicMock()
    spark_mock.readStream = read_stream
    read_stream.format.return_value = read_stream
    read_stream.option.return_value = read_stream

    df_kafka = MagicMock()
    read_stream.load.return_value = df_kafka

    messages_df = MagicMock()
    df_kafka.selectExpr.return_value = messages_df

    df_events_raw = MagicMock()
    messages_df.select.return_value = df_events_raw

    # Chaining transformations
    df_events_raw.select.return_value = df_events_raw
    df_events_raw.withColumn.return_value = df_events_raw
    df_events_raw.filter.return_value = df_events_raw

    # Mock the writeStream chain and capture the callback
    write_stream = MagicMock()
    df_events_raw.writeStream = write_stream
    write_stream.option.return_value = write_stream
    write_stream.trigger.return_value = write_stream

    callback_holder = {}

    def record_callback(fn):
        callback_holder["fn"] = fn
        return write_stream

    write_stream.foreachBatch.side_effect = record_callback

    query_mock = MagicMock()
    write_stream.start.return_value = query_mock
    query_mock.awaitTermination.return_value = None

    with patch("src.streaming.stream_consumer.SparkSession") as SparkSessionMock, \
         patch(
             "src.streaming.stream_consumer.col",
             side_effect=lambda c: MagicMock(isNotNull=MagicMock(return_value=MagicMock())),
         ), \
         patch(
             "src.streaming.stream_consumer.from_json",
             side_effect=lambda col_obj, schema: MagicMock(alias=lambda name: MagicMock()),
         ), \
         patch("src.streaming.stream_consumer.to_timestamp", side_effect=lambda c: f"to_timestamp({c})"), \
         patch("src.streaming.stream_consumer.to_date", side_effect=lambda c: f"to_date({c})"):
        SparkSessionMock.builder = builder_mock

        env = {
            "SPARK_MASTER_URL": "spark://test:7077",
            "KAFKA_SERVERS": "kafka:1234",
            "BRONZE_PATH": "/tmp/bronze",
            "SILVER_PATH": "/tmp/silver",
            "CHECKPOINT_PATH": "/tmp/check",
            "SPARK_SQL_SHUFFLE_PARTITIONS": "5",
        }
        with patch.dict(os.environ, env, clear=True):
            main()

    # Ensure process_batch was captured and can be invoked
    assert "fn" in callback_holder
    process_batch = callback_holder["fn"]

    empty_df = MagicMock()
    empty_df.isEmpty.return_value = True
    empty_df.write = MagicMock()

    process_batch(empty_df, 0)

    empty_df.write.parquet.assert_not_called()
