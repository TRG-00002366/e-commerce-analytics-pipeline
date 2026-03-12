"""
Purpose:
    Consumes real-time Kafka streams of order events and writes to Bronze layer.

Responsibilities:
    - Connect to Kafka topic `order_events`.
    - Deserialize JSON messages into Spark DataFrames using StructType schema enforcement.
    - Apply deduplication (via deduplication.py) and handle malformed records.
    - Write raw events to Bronze layer (Parquet, partitioned by date & event_type).

Important Behavior:
    - Micro-batch trigger interval configurable (default 1 minute).
    - Logs streaming metrics: processed record count.

Design Notes:
    - Modular design to allow streaming transformations or joins with additional topics in future.
    - Works as a foundation for downstream batch ETL.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os

from deduplication import deduplicate_events
from src.util.logging import get_logger

logger = get_logger(__name__)

# Schema for order events
order_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("shipping_type", StringType(), True),
    StructField("region", StringType(), True),
    StructField("timestamp", StringType(), True)
])

def main():

    spark = SparkSession.builder \
        .appName("OrderEventStreamConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    kafka_servers = os.getenv("KAFKA_SERVERS", "localhost:9092")
    bronze_path = os.getenv("BRONZE_PATH", "data/raw/")
    checkpoint_path = os.path.join(bronze_path, "checkpoint")

    def process_batch(df, batch_id):
        """
        Logs batch processing statistics and writes to Bronze layer with partitioning.
        """
        if df.isEmpty():
            logger.info(f"Batch {batch_id} empty")
            return
        
        count = df.count()
        logger.info(f"Processed batch {batch_id} with {count} records")

        # Write the batch to Parquet with partitioning
        df.write.mode("append") \
            .partitionBy("date", "event_type") \
            .parquet(bronze_path)
    
    logger.info("Starting Kafka stream consumer")

    # Read from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "order_events") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Extract JSON payload
    messages_df = df_kafka.selectExpr("CAST(value AS STRING) as json")

    # Parse JSON
    df_events_raw = messages_df \
        .select(from_json(col("json"), order_schema).alias("data")) \
        .select("data.*") \
        .filter(col("event_id").isNotNull())

    # Convert timestamp
    df_events = df_events_raw \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("date", to_date(col("timestamp")))

    # Deduplicate (streaming-safe)
    df_events_dedup = deduplicate_events(df_events)

    # Write valid events (Bronze)
    query_valid = df_events_dedup.writeStream \
        .format("parquet") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="1 minute") \
        .foreachBatch(process_batch) \
        .start()

    logger.info("Streaming query started")

    # Wait for termination
    query_valid.awaitTermination()


if __name__ == "__main__":
    main()