from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from batch.df_etl import df_etl
from datetime import datetime, timedelta

default_args = {
    "owner": "rev",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="df_batch_etl_pipeline",
    default_args=default_args,
    description="Batch ETL for Gold layer using PySpark",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "batch", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    # Check if Silver data exists
    check_silver_data = BashOperator(
        task_id="check_silver_data",
        bash_command="""
        if [ ! -d "/opt/data/silver" ]; then
            echo "Silver data not found!"
            exit 1
        else
            echo "Silver data exists"
        fi
        """,
    )

    run_df_etl = PythonOperator(
        task_id="run_df_etl",
        python_callable=df_etl
    )

    end = EmptyOperator(task_id="end")

    start >> check_silver_data >> run_df_etl >> end