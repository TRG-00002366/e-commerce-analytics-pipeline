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