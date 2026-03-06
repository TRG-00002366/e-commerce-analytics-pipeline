"""
Purpose:
    Implements the RDD-based batch ETL transformations on raw Amazon order events.

Responsibilities:
    - Load raw Parquet files from Bronze layer into RDDs.
    - Filter out CANCELLED orders.
    - Compute total revenue per product_id using key-value pair RDDs.
    - Track malformed/bad records using Spark accumulators.
    - Save transformed results to Silver/Gold layer (text or Parquet).

Important Behavior:
    - Applies map, filter, reduceByKey transformations.
    - Ensures idempotency by avoiding duplicate processing of the same order_id.
    - Logs summary statistics for validation (e.g., total revenue, number of records processed).

Design Notes:
    - Focused on demonstrating low-level Spark capabilities (RDD transformations).
    - Works in conjunction with df_etl.py for full batch analytics.
    - Designed to be modular so additional RDD-based transformations can be added.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import os
from src.util.logging import get_logger

logger = get_logger(__name__)

spark = SparkSession.builder.appName("AmazonRDD_ETL").getOrCreate()

RAW_PATH = os.getenv("RAW_PATH", "data/raw/")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "data/transformed/rdd_revenue/")

malformed_records = spark.sparkContext.accumulator(0)
processed_orders = spark.sparkContext.accumulator(0)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

def parse_row(row):
    try:
        order_id = row["order_id"]
        product_id = row["product_id"]
        quantity = int(row["quantity"])
        unit_price = float(row["unit_price"])
        discount = float(row.get("discount", 0.0))
        event_type = row.get("event_type", "")
        status = "CREATED"
        if "CANCELLED" in event_type:
            status = "CANCELLED"
        elif "RETURNED" in event_type:
            status = "RETURNED"
        processed_orders.add(1)
        return (order_id, (product_id, quantity * (unit_price - discount), status))
    except KeyError as e:
        logger.error(f"Missing key: {e}")
        malformed_records.add(1)
        return None
    except (ValueError, TypeError) as e:
        logger.error(f"Type error: {e}")
        malformed_records.add(1)
        return None

df_raw = spark.read.schema(schema).parquet(os.path.join(RAW_PATH, "*"))
rdd_raw = df_raw.rdd.map(lambda row: row.asDict())
rdd_parsed = rdd_raw.map(parse_row).filter(lambda x: x is not None)
rdd_deduped = rdd_parsed.reduceByKey(lambda a, b: a)  # Deduplicate
rdd_valid = rdd_deduped.filter(lambda x: x[1][2] != "CANCELLED")
rdd_valid = rdd_valid.repartition(10)  # Repartition for performance on large data
rdd_revenue = rdd_valid.map(lambda x: (x[1][0], x[1][1])).reduceByKey(lambda a, b: a + b)
rdd_revenue.map(lambda x: f"{x[0]},{x[1]}").saveAsTextFile(OUTPUT_PATH)

logger.info(f"Processed orders: {processed_orders.value}")
logger.info(f"Malformed records: {malformed_records.value}")

spark.stop()