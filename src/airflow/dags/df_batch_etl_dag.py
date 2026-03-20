from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from batch.df_etl import df_etl
from datetime import datetime, timedelta

default_args = {
    "owner": "rev",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="df_batch_etl_pipeline",
    default_args=default_args,
    description="Batch ETL for Gold layer using PySpark",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["spark", "batch", "etl"],
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)

    # ✅ Check if Silver data exists
    check_silver_data = BashOperator(
        task_id="check_silver_data",
        bash_command="""
        if [ ! -d "/opt/data/silver" ]; then
            echo "❌ Silver data not found!"
            exit 1
        else
            echo "✅ Silver data exists"
        fi
        """,
        dag=dag
    )

    # ✅ Stable Spark submit (this is the key part)
    # run_df_etl = BashOperator(
    #     task_id="run_df_etl",
    #     bash_command="""
    #     echo "🚀 Starting Spark ETL job..."

    #     /opt/spark/bin/spark-submit \
    #     --master spark://spark-master:7077 \
    #     --deploy-mode client \
    #     /opt/batch/df_etl.py

    #     echo "✅ Spark ETL completed"
    #     """
    # )

    # run_df_etl = SparkSubmitOperator(
    #     task_id="run_df_etl",
    #     conn_id="spark-conn",
    #     application="/opt/batch/df_etl.py",
    #     conf={
    #         "spark.master": "spark://spark-master:7077"
    #     },
    #     verbose=True,
    #     dag=dag
    # )

    run_df_etl = PythonOperator(
        task_id="run_df_etl",
        python_callable=df_etl
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> check_silver_data >> run_df_etl >> end