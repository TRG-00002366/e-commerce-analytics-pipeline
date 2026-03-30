from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.docker.operators.docker import DockerOperator
from kafka_jobs.producer import stream_events
from kafka_jobs.create_topics import create_topic
from datetime import datetime, timedelta

default_args = {
    "owner": "rev",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="kafka_setup_workflow",
    default_args=default_args,
    description="Create Kafka topic and launch producer.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["kafka", "topic", "producer"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    task_create_topic = PythonOperator(
        task_id="create_topic",
        python_callable=create_topic
    )

    task_run_producer = PythonOperator(
        task_id="run_producer",
        python_callable=stream_events
    )

    # task_run_consumer = SparkSubmitOperator(
    #     task_id="run_consumer",
    #     application="/opt/spark-apps/streaming/stream_consumer.py",
    #     conn_id="spark_local",
    #     packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    #     conf={
    #         "spark.master": "spark://spark-master:7077"
    #     },
    #     env_vars={
    #         "KAFKA_SERVERS": "kafka:29092",
    #         "BRONZE_PATH": "/opt/data/bronze",
    #         "SILVER_PATH": "/opt/data/silver",
    #         "CHECKPOINT_PATH": "/opt/data/checkpoint"
    #     }
    # )

    task_run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            /opt/spark-apps/streaming/stream_consumer.py"
    )

    end = EmptyOperator(task_id="end")

    start >> task_create_topic >> task_run_producer >> task_run_consumer >> end