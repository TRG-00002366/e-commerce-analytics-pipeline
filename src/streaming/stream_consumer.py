"""
    Consumes real-time Kafka streams of order events and writes to Bronze and Silver layers.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from util.deduplication import deduplicate_events
from util.logging import get_logger

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
    # Spark Session (Docker Spark cluster)
    spark = SparkSession.builder \
        .appName("OrderEventStreamConsumer") \
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")) \
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")) \
        .getOrCreate()

    # Env vars
    kafka_servers = os.getenv("KAFKA_SERVERS", "kafka:29092")
    bronze_path = os.getenv("BRONZE_PATH", "/opt/data/bronze")
    silver_path = os.getenv("SILVER_PATH", "/opt/data/silver")
    checkpoint_path = os.getenv("CHECKPOINT_PATH", "/opt/data/checkpoint")

    logger.info(f"Kafka Servers: {kafka_servers}")
    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver path: {silver_path}")
    logger.info(f"Checkpoint path: {checkpoint_path}")

    # Micro-batch logic
    def process_batch(df, batch_id):
        if df.isEmpty():
            logger.info(f"Batch {batch_id} empty")
            return

        count = df.count()
        logger.info(f"Processing batch {batch_id}, records: {count}")

        # Bronze 
        df.write.mode("append") \
            .partitionBy("date", "event_type") \
            .parquet(bronze_path)

        # Silver
        df_clean = deduplicate_events(df)

        df_clean.write.mode("append") \
            .partitionBy("date", "event_type") \
            .parquet(silver_path)

    # Kafka Source
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "order_events") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON
    messages_df = df_kafka.selectExpr("CAST(value AS STRING) as json")

    df_events_raw = messages_df \
        .select(from_json(col("json"), order_schema).alias("data")) \
        .select("data.*") \
        .filter(col("event_id").isNotNull())

    # Add timestamp + partition column
    df_events = df_events_raw \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("date", to_date(col("timestamp")))

    # Streaming Query
    query = df_events.writeStream \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="1 minute") \
        .foreachBatch(process_batch) \
        .start()

    logger.info("Streaming query started...")
    query.awaitTermination()


if __name__ == "__main__":
    main()