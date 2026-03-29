"""
Implements the RDD-based batch ETL transformations on Amazon order events
sourced from the Bronze layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)
import os
from util.logging import get_logger

logger = get_logger(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("AmazonRDD_ETL").getOrCreate()

# Paths updated to reflect Medallion Architecture
BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/data/bronze")
SILVER_OUTPUT_PATH = os.getenv("SILVER_PATH", "/opt/data/silver/rdd_revenue")

malformed_records = spark.sparkContext.accumulator(0)
processed_orders = spark.sparkContext.accumulator(0)

# Define schema for validation (matches the data produced by stream_consumer.py)
schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)


def parse_row(row):
    """Extract and validate order fields, tracking status for filtering."""
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
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Parsing error: {e}")
        malformed_records.add(1)
        return None


# 1. Load data from the Bronze layer
# Spark automatically handles the 'date' and 'event_type' partitions created by the streamer.
df_bronze = spark.read.schema(schema).parquet(BRONZE_PATH)
rdd_raw = df_bronze.rdd.map(lambda row: row.asDict())

# 2. Parse and validate each row
rdd_parsed = rdd_raw.map(parse_row).filter(lambda x: x is not None)

# 3. Deduplication and Filtering
rdd_deduped = rdd_parsed.reduceByKey(lambda a, b: a)
rdd_valid = rdd_deduped.filter(lambda x: x[1][2] != "CANCELLED")

# 4. Optimization for shuffle
rdd_valid
