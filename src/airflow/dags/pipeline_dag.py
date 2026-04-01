from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from kafka_jobs.producer import stream_events
from kafka_jobs.create_topics import create_topic
from datetime import datetime, timedelta
import os

BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "/opt/data/silver")
BRONZE_LOAD_STAGE = os.getenv("BRONZE_LOAD_STAGE", "BRONZE_LOAD_STAGE")
SILVER_LOAD_STAGE = os.getenv("SILVER_LOAD_STAGE", "SILVER_LOAD_STAGE")

def put_to_bronze_stage():
    hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
    put_sql = f"PUT file://{BRONZE_PATH}/*.parquet @BRONZE.{BRONZE_LOAD_STAGE} OVERWRITE=TRUE;"
    hook.run(put_sql)
    print(f"Uploaded {BRONZE_PATH} to @BRONZE.{BRONZE_LOAD_STAGE} (overwritten)")

def put_to_silver_stage():
    hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
    put_sql = f"PUT file://{SILVER_PATH}/*.parquet @SILVER.{SILVER_LOAD_STAGE} OVERWRITE=TRUE;"
    hook.run(put_sql)
    print(f"Uploaded {SILVER_PATH} to @SILVER.{SILVER_LOAD_STAGE} (overwritten)")

default_args = {
    "owner": "rev",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pipeline_dag",
    default_args=default_args,
    description="Create Kafka topic and launch producer.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["kafka", "topic", "producer"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    task_clean_data_dir = BashOperator(
        task_id="clean_data_dir",
        bash_command="rm -rf /opt/data/silver /opt/data/bronze /opt/data/gold /opt/data/checkpoint"
    )
    
    task_create_topic = PythonOperator(
        task_id="create_topic",
        python_callable=create_topic
    )

    task_run_producer = PythonOperator(
        task_id="run_producer",
        python_callable=stream_events
    )

    task_run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            /opt/spark-apps/streaming/stream_consumer.py"
    )

    task_put_to_bronze_stage = PythonOperator(
        task_id="put_to_bronze_stage",
        python_callable=put_to_bronze_stage,
    )

    task_put_to_silver_stage = PythonOperator(
        task_id="put_to_silver_stage",
        python_callable=put_to_silver_stage,
    )

    task_truncate_bronze_stg = SnowflakeOperator(
        task_id="truncate_bronze_stg",
        sql="TRUNCATE TABLE IF EXISTS BRONZE.stg_orders;",
        snowflake_conn_id="my_snowflake_conn"
    )

    task_truncate_silver_stg = SnowflakeOperator(
        task_id="truncate_silver_stg",
        sql="TRUNCATE TABLE IF EXISTS SILVER.stg_cleaned_orders;",
        snowflake_conn_id="my_snowflake_conn"
    )

    task_copy_into_bronze_stg = SnowflakeOperator(
        task_id="copy_into_bronze_stg",
        sql=f"""
            COPY INTO BRONZE.stg_orders (raw_data)
            FROM (
                SELECT OBJECT_CONSTRUCT(*)
                FROM @BRONZE.{BRONZE_LOAD_STAGE}
            )
            FILE_FORMAT = (TYPE = 'PARQUET')
            PATTERN = '.*\\.parquet'
            FORCE=FALSE
            PURGE = TRUE;
        """,
        snowflake_conn_id="my_snowflake_conn"
    )

    task_copy_into_silver_stg = SnowflakeOperator(
        task_id="copy_into_silver_stg",
        sql=f"""
            COPY INTO SILVER.stg_cleaned_orders
            FROM @SILVER.{SILVER_LOAD_STAGE}
            FILE_FORMAT = (TYPE='PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PATTERN = '.*\\.parquet'
            FORCE=FALSE
            PURGE=TRUE;
        """,
        snowflake_conn_id="my_snowflake_conn"
    )

    task_merge_into_bronze_target = SnowflakeOperator(
        task_id="merge_into_bronze_target",
        sql="""
            INSERT INTO BRONZE.orders (raw_data)
            SELECT raw_data
            FROM BRONZE.stg_orders;
        """,
        snowflake_conn_id="my_snowflake_conn"
    )

    task_merge_into_silver_target = SnowflakeOperator(
        task_id="merge_into_silver_target",
        sql="""
            MERGE INTO SILVER.cleaned_orders AS target
            USING SILVER.stg_cleaned_orders AS source
            ON target.event_id = source.event_id
            WHEN MATCHED AND source.event_timestamp > target.event_timestamp THEN
                UPDATE SET
                    event_type = source.event_type,
                    order_id = source.order_id,
                    customer_id = source.customer_id,
                    customer_segment = source.customer_segment,
                    product_id = source.product_id,
                    product_name = source.product_name,
                    category = source.category,
                    quantity = source.quantity,
                    unit_price = source.unit_price,
                    discount = source.discount,
                    payment_method = source.payment_method,
                    shipping_type = source.shipping_type,
                    region = source.region,
                    event_timestamp = source.event_timestamp
            WHEN NOT MATCHED THEN
                INSERT (
                    event_id,
                    event_type,
                    order_id,
                    customer_id,
                    customer_segment,
                    product_id,
                    product_name,
                    category,
                    quantity,
                    unit_price,
                    discount,
                    payment_method,
                    shipping_type,
                    region,
                    event_timestamp
                )
                VALUES (
                    source.event_id,
                    source.event_type,
                    source.order_id,
                    source.customer_id,
                    source.customer_segment,
                    source.product_id,
                    source.product_name,
                    source.category,
                    source.quantity,
                    source.unit_price,
                    source.discount,
                    source.payment_method,
                    source.shipping_type,
                    source.region,
                    source.event_timestamp
                );
        """,
        snowflake_conn_id="my_snowflake_conn"
    )

    end = EmptyOperator(task_id="end")

    # start >> task_create_topic >> task_run_producer >> task_run_consumer >> task_put_to_bronze_stage >> \
    # task_put_to_silver_stage >> task_truncate_bronze_stg >> task_truncate_silver_stg >> task_copy_into_bronze_stg >> \
    # task_copy_into_silver_stg >> task_merge_into_bronze_target >> task_merge_into_silver_target >> end

    # start >> task_create_topic >> task_run_producer >> task_run_consumer >> end

    # Start
    start >> task_clean_data_dir

    # Kafka stage
    task_clean_data_dir >> task_create_topic >> task_run_producer >> task_run_consumer

    # Bronze stage
    task_run_consumer >> task_put_to_bronze_stage
    task_put_to_bronze_stage >> task_truncate_bronze_stg >> task_copy_into_bronze_stg >> task_merge_into_bronze_target

    # Silver stage
    task_run_consumer >> task_put_to_silver_stage
    task_put_to_silver_stage >> task_truncate_silver_stg >> task_copy_into_silver_stg >> task_merge_into_silver_target

    # End
    task_merge_into_bronze_target >> end
    task_merge_into_silver_target >> end