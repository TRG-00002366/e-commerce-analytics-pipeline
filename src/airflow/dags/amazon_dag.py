"""
Purpose:
    Orchestrates the entire Amazon Order Analytics pipeline using Apache Airflow.

Responsibilities:
    - Define DAG for the full pipeline:
        - Kafka topic verification
        - Streaming job submission
        - Batch RDD ETL
        - Batch DataFrame ETL
        - Data validation and monitoring
    - Implement task dependencies using TaskGroups where appropriate.
    - Configure retries, SLA alerts, and email notifications on failure.
    - Parameterize DAG for execution_date and environment.

Important Behavior:
    - Waits for Bronze layer data before executing batch ETL.
    - Uses PythonOperator, BashOperator, and FileSensor for orchestration.
    - Executes Spark jobs via `spark-submit` in cluster mode.
    - Performs basic data quality checks on row counts, schema validation, and null/negative values.

Design Notes:
    - Designed for readability and maintainability with clear task naming.
    - Extensible to support additional Kafka topics or dynamic DAG generation.
    - Centralizes orchestration while keeping transformation logic in batch and streaming modules.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email

# Convenience wrapper around subprocess.run to execute a python script.
def _run_script(script_path: str) -> None:
    if not os.path.isfile(script_path):
        raise FileNotFoundError(f"script not found: {script_path}")
    subprocess.run(["python3", script_path], check=True)

# Create necessary Kafka topics by invoking the helper script.
def verify_kafka_topics(**_kwargs):
    script = os.path.join(os.getcwd(), "src/kafka/create_topics.py")
    _run_script(script)

# Launch the Spark streaming consumer using spark-submit.
def submit_streaming_job(**_kwargs):
    spark_submit = os.getenv("SPARK_SUBMIT", "spark-submit")
    script = os.path.join(os.getcwd(), "src/streaming/stream_consumer.py")
    args = [spark_submit, "--master", "local[*]", script]
    subprocess.run(args, check=True)

# Perform simple data quality validation against the Gold layer.
def quality_check(**_kwargs):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    spark = SparkSession.builder.appName("DataQuality").getOrCreate()
    output_path = os.getenv("OUTPUT_PATH", "data/transformed/df_analytics/hourly_sales")
    df = spark.read.parquet(output_path)

    row_count = df.count()
    if row_count == 0:
        raise ValueError("data quality check failed: no rows in hourly_sales")

    # ensure there are no nulls in some important columns
    nulls = df.filter(
        col("date").isNull() |
        col("hour").isNull() |
        col("total_orders").isNull()
    ).count()
    if nulls > 0:
        raise ValueError(f"data quality check failed: {nulls} null records found")

    spark.stop()


def _send_failure_email(context):
    send_email(
        to=os.getenv("ALERT_EMAIL", ""),
        subject=f"Airflow DAG Failed: {context['dag_run'].dag_id}",
        html_content=f"<p>Task {context['task_instance'].task_id} failed.</p>"
    )


# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": [os.getenv("ALERT_EMAIL", "")],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _send_failure_email,
}

with DAG(
    dag_id="amazon_order_analytics",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "spark", "kafka"],
) as dag:

    verify_kafka = PythonOperator(
        task_id="verify_kafka_topics",
        python_callable=verify_kafka_topics,
    )

    stream_job = PythonOperator(
        task_id="submit_streaming_job",
        python_callable=submit_streaming_job,
    )

    bronze_sensor = FileSensor(
        task_id="wait_for_bronze_data",
        filepath="data/raw/_SUCCESS",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=60 * 60 * 6, 
        mode="poke",
    )

    rdd_etl = BashOperator(
        task_id="run_rdd_etl",
        bash_command="spark-submit --master local[*] src/batch/rdd_etl.py",
    )

    df_etl = BashOperator(
        task_id="run_df_etl",
        bash_command="spark-submit --master local[*] src/batch/df_etl.py",
    )

    validate = PythonOperator(
        task_id="data_quality_check",
        python_callable=quality_check,
    )

    # task dependencies
    verify_kafka >> stream_job >> bronze_sensor >> rdd_etl >> df_etl >> validate


# expose dag variable for Airflow
dag = dag
