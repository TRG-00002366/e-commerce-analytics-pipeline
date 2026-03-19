from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, hour, when, row_number, date_format, coalesce
from pyspark.sql.window import Window
import os

"""
Performs DataFrame and Spark SQL-based batch ETL for analytics-ready datasets.
"""

def df_etl():
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("AmazonDF_ETL").getOrCreate()
    
    # Paths
    RAW_PATH = os.getenv("RAW_PATH", "data/raw/")
    REGIONS_PATH = os.getenv("REGIONS_PATH", "data/regions.csv")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "data/transformed/df_analytics/")
    try:
        df_orders = spark.read.parquet(os.path.join(RAW_PATH, "order_events"))
        df_regions = spark.read.csv(REGIONS_PATH, header=True, inferSchema=True)
        
        df_orders = df_orders.withColumn("order_status", 
            when(col("event_type") == "ORDER_CANCELLED", "CANCELLED")
            .when(col("event_type") == "ORDER_RETURNED", "RETURNED")
            .otherwise("CREATED")
        )
        
        df_orders = df_orders.filter(col("order_status") != "CANCELLED").na.fill({"discount": 0.0, "quantity": 1, "unit_price": 0.0})
        
        df_orders = df_orders.join(df_regions, df_orders.region == df_regions.region_code, "left") \
                             .withColumnRenamed("region_name", "region_name")
        
        # Cache and repartition for performance on large data
        df_orders.cache()
        df_orders = df_orders.repartition(10)
        
        # Hourly Sales Summary
        df_hourly = df_orders.withColumn("hour", hour(col("timestamp"))) \
            .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
            .groupBy("date", "hour") \
            .agg(
                count("order_id").alias("total_orders"),
                spark_sum(coalesce((col("unit_price") - col("discount")) * col("quantity"), 0)).alias("total_revenue"),
                avg(coalesce((col("unit_price") - col("discount")) * col("quantity"), 0)).alias("avg_order_value")
            )
        
        df_hourly.write.mode("overwrite").partitionBy("date").csv(os.path.join(OUTPUT_PATH, "hourly_sales"))
        
        # Top 10 Products by Quantity Sold per Category
        window_cat = Window.partitionBy("category").orderBy(col("total_qty").desc())
        df_top_products = df_orders.groupBy("category", "product_id", "product_name") \
            .agg(spark_sum(coalesce("quantity", 0)).alias("total_qty")) \
            .withColumn("rank", row_number().over(window_cat)) \
            .filter(col("rank") <= 10)
        df_top_products.write.mode("overwrite").csv(os.path.join(OUTPUT_PATH, "top_products"))
        
        # Regional Revenue
        df_regional = df_orders.groupBy("region_name") \
            .agg(spark_sum(coalesce((col("unit_price") - col("discount")) * col("quantity"), 0)).alias("revenue"))
        df_regional.write.mode("overwrite").csv(os.path.join(OUTPUT_PATH, "regional_revenue"))
        
        # Customer Segment KPIs
        df_segment = df_orders.groupBy("customer_segment") \
            .agg(
                spark_sum(coalesce((col("unit_price") - col("discount")) * col("quantity"), 0)).alias("revenue"),
                avg(coalesce("quantity", 0)).alias("avg_basket_size")
            )
        df_segment.write.mode("overwrite").csv(os.path.join(OUTPUT_PATH, "customer_segment"))
        
        # Return & Cancellation Rates per Category
        df_status = df_orders.groupBy("category") \
            .agg(
                count(when(col("order_status") == "RETURNED", True)).alias("returns"),
                count(when(col("order_status") == "CANCELLED", True)).alias("cancellations"),
                count("order_id").alias("total_orders")
            )
        df_status = df_status.withColumn("return_rate", coalesce(col("returns") / col("total_orders"), 0)) \
                             .withColumn("cancellation_rate", coalesce(col("cancellations") / col("total_orders"), 0))
        df_status.write.mode("overwrite").csv(os.path.join(OUTPUT_PATH, "status_metrics"))
        
    except Exception as e:
        print(f"Error in ETL: {e}")
        raise
    
    spark.stop()