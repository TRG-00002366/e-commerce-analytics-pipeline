from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from batch.df_etl import df_etl
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="End-to-end e-commerce Analytics: Kafka to Spark ETL",
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    produce_events = BashOperator(
        task_id="produce_order_events",
        bash_command="python /opt/project/kafka_jobs/producer.py --bootstrap-servers kafka:29092 --num-events 500",
    )

    run_streaming_consumer = BashOperator(
        task_id="run_streaming_consumer",
        bash_command=(
            "spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            "/opt/project/streaming/stream_consumer.py "
            "--bootstrap-servers kafka:29092 --duration 120"
        ),
    )

    # STEP 5: RDD-based Batch ETL (Using /opt/project path)
    run_batch_rdd_etl = BashOperator(
        task_id="run_batch_rdd_etl",
        bash_command="spark-submit /opt/project/batch/rdd_etl.py",
    )

    # STEP 6: DataFrame-based Batch ETL (Using /opt/project path)
    run_batch_df_etl = BashOperator(
        task_id="run_batch_df_etl",
        bash_command="spark-submit /opt/project/batch/df_etl.py",
    )

    end = EmptyOperator(task_id="end")

    # --- DAG Structure ---
    (
        start
        >> produce_events
        >> run_streaming_consumer
        >> [run_batch_rdd_etl, run_batch_df_etl]
        >> end
    )