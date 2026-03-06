"""
Purpose:
    Houses reusable PySpark aggregation functions for batch ETL jobs.

Responsibilities:
    - Define functions for calculating KPIs: hourly sales, top products, regional revenue, customer segment metrics.
    - Provide pivoting logic for order status breakdown and return/cancellation rates.
    - Abstract windowing and grouping logic to be used in df_etl.py.

Important Behavior:
    - Functions assume input DataFrame follows Bronze layer schema.
    - Optimized for caching and re-use to reduce computation in multiple transformations.
    - Handles missing or null values gracefully to maintain data quality.

Design Notes:
    - Separates business logic from ETL orchestration.
    - Designed to support additional KPIs in the future.
    - Functions are unit-testable independently from Spark job submission.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, avg, count, hour, when, row_number, coalesce
from pyspark.sql.window import Window
from typing import str

# Hourly Sales Summary
def hourly_sales_summary(df: DataFrame, timestamp_col: str = "timestamp") -> DataFrame:
    """
    Computes total orders, total revenue, and average order value per hour.
    Handles nulls.
    """
    df_hourly = df.withColumn("hour", hour(col(timestamp_col))) \
        .groupBy("hour") \
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum(coalesce((col("unit_price") - col("discount")) * col("quantity"), 0)).alias("total_revenue"),
            avg(coalesce((col("unit_price") - col("discount")) * col("quantity"), 0)).alias("avg_order_value")
        )
    return df_hourly

# Top N Products per Category
def top_n_products(df: DataFrame, category_col: str = "category", n: int = 10) -> DataFrame:
    """
    Ranks products by total quantity sold within each category and returns top N.
    Handles nulls.
    """
    window_cat = Window.partitionBy(category_col).orderBy(col("total_qty").desc())
    df_top = df.groupBy(category_col, "product_id", "product_name") \
        .agg(spark_sum(coalesce("quantity", 0)).alias("total_qty")) \
        .withColumn("rank", row_number().over(window_cat)) \
        .filter(col("rank") <= n)
    return df_top

# Regional Revenue
def regional_revenue(df: DataFrame, region_col: str = "region_name") -> DataFrame:
    """
    Aggregates total revenue by region.
    Handles nulls.
    """
    df_region = df.groupBy(region_col) \
        .agg(spark_sum(coalesce((col("unit_price") - col("discount")) * col("quantity"), 0)).alias("revenue"))
    return df_region

# Customer Segment KPIs
def customer_segment_kpis(df: DataFrame, segment_col: str = "customer_segment") -> DataFrame:
    """
    Computes revenue and average basket size per customer segment.
    Handles nulls.
    """
    df_segment = df.groupBy(segment_col) \
        .agg(
            spark_sum(coalesce((col("unit_price") - col("discount")) * col("quantity"), 0)).alias("revenue"),
            avg(coalesce("quantity", 0)).alias("avg_basket_size")
        )
    return df_segment

# Order Status Metrics (Return & Cancellation Rates)
def order_status_metrics(df: DataFrame, category_col: str = "category") -> DataFrame:
    """
    Calculates return and cancellation counts and rates per category.
    Handles nulls.
    """
    df_status = df.groupBy(category_col) \
        .agg(
            count(when(col("order_status") == "RETURNED", True)).alias("returns"),
            count(when(col("order_status") == "CANCELLED", True)).alias("cancellations"),
            count("order_id").alias("total_orders")
        )
    df_status = df_status.withColumn("return_rate", coalesce(col("returns") / col("total_orders"), 0)) \
                         .withColumn("cancellation_rate", coalesce(col("cancellations") / col("total_orders"), 0))
    return df_status