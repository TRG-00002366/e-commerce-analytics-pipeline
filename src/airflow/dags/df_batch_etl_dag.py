from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
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
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "batch", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

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
        """
    )

    # ✅ Stable Spark submit (this is the key part)
    run_df_etl = BashOperator(
        task_id="run_df_etl",
        bash_command="""
        echo "🚀 Starting Spark ETL job..."

        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/spark-apps/batch/df_etl.py

        echo "✅ Spark ETL completed"
        """
    )

    end = EmptyOperator(task_id="end")

    start >> check_silver_data >> run_df_etl >> end