# Project 1: Real-Time Amazon Order Analytics Pipeline

## Overview

Build an end-to-end data engineering pipeline that ingests real-time Amazon order events via Kafka, processes them with PySpark, applies data quality validations, and orchestrates the entire workflow using Apache Airflow.

---

## Business Scenario

Amazon, a global multi-channel e-commerce retailer, wants to:

1. **Stream** real-time orders, payments, returns, and cancellations across its website, mobile app, and international marketplaces.
2. **Process** the raw events to compute operational and business KPIs in near real-time, including:
    - Gross Merchandise Value
    - Net revenue
    - Top-selling products per category and region
    - Return rate and cancellation rate
    - Customer segment revenue (e.g., Prime vs. Non-Prime)
    - Average basket size per region
4. **Persist** both raw and transformed data to storage (local filesystem or S3) for analytics and reporting.
5. **Orchestrate** and monitor the full pipeline with retry policies, SLA alerts, and data validation.

---

## Architecture

```
┌──────────────┐       ┌─────────────────┐       ┌─────────────────────┐
│ Amazon Event │       │    Kafka        │       │  PySpark Streaming   │
│  Simulator   │──────▶│  (Topic:       │──────▶│  Consumer / ETL      │
│  (Producer)  │       │  order_events   │       │  (Spark Structured   │
│              │       │                 │       │   Streaming)          │
└──────────────┘       └─────────────────┘       └──────────┬────────────┘
                                                        │
                                                        ▼
                                              ┌─────────────────────┐
                                              │  Raw Data Layer     │
                                              │  (Parquet / JSON)   │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  PySpark Batch ETL  │
                                              │  (Aggregations,     │
                                              │   Joins, Filters)   │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  Transformed Data   │
                                              │  (Parquet / CSV)    │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  Airflow DAG        │
                                              │  (Orchestration)    │
                                              └─────────────────────┘
```

---

## Tech Stack

| Technology     | Purpose                                      | Curriculum Week |
|----------------|----------------------------------------------|:---------------:|
| PySpark (RDDs) | Low-level data processing & custom transforms| Week 1          |
| PySpark (SQL)  | DataFrame operations, aggregations, joins    | Week 2          |
| Apache Kafka   | Real-time event ingestion (producer/consumer)| Week 3          |
| Spark Streaming| Consuming Kafka topics in near real-time     | Week 3          |
| Apache Airflow | DAG-based job orchestration & scheduling     | Week 4          |

---

## Detailed Requirements

### Module 1 — Kafka Producer (Week 3)

**Goal:** Simulate Amazon-like order and payment events.

- Create a Kafka topics named `order_events`.
- Write a Python Kafka producer (`producer.py`) that generates JSON order events:
  ```json
  {
    "event_id": "EVT-90001",
    "event_type": "ORDER_CREATED",
    "order_id": "ORD-10042",
    "customer_id": "CUST-301",
    "customer_segment": "PRIME",
    "product_id": "PROD-88",
    "product_name": "Wireless Mouse",
    "category": "Electronics",
    "quantity": 2,
    "unit_price": 29.99,
    "discount": 5.00,
    "payment_method": "CREDIT_CARD",
    "shipping_type": "PRIME_EXPRESS",
    "region": "US-East",
    "timestamp": "2026-02-19T10:32:00Z"
  }
  ```
- Use `event_type` values: `ORDER_CREATED`, `ORDER_CANCELLED`, `ORDER_RETURNED`.
- Produce at least **500 events** with randomized data using the `Faker` library.

---

### Module 2 — Spark Streaming Consumer (Week 3)

**Goal:** Consume and persist the raw Kafka stream with schema enforcement, deduplication, and bad record handling.

- Write a PySpark Structured Streaming job (`stream_consumer.py`).
- Read from the`order_events` Kafka topic.
- Deserialize JSON messages into a Spark DataFrame.
- Implement:
  - Deduplication by event_id
  - Malformed record handling → save to `data/bad_records/`
- Write the raw data to a **Parquet** sink partitioned by `date` (derived from `timestamp`).

---

### Module 3 — Batch ETL with PySpark (Weeks 1 & 2)

**Goal:** Transform raw data into analytics-ready datasets.

#### 3A — RDD-Based Processing (Week 1)

- Load the raw Parquet data as an RDD.
- Use RDD transformations (`map`, `filter`, `reduceByKey`) to:
  - Filter out `CANCELLED` orders.
  - Compute total revenue per `product_id` using key-value pair RDDs.
  - Track malformed/bad records using accumulators.
- Save the result as a text file.

#### 3B — DataFrame / Spark SQL Processing (Week 2)

- Load the raw Parquet data into a Spark DataFrame.
- Perform the following transformations:
  1. **Hourly Sales Summary** — Group by hour, compute `total_orders`, `total_revenue`, `avg_order_value`.
  2. **Top 10 Products** — Rank products by total quantity sold using Spark SQL window functions.
  3. **Regional Revenue** — Join orders with a static `regions.csv` reference dataset to enrich region names, then aggregate revenue by region.
  4. **Customer Segment KPIs** — revenue, average basket size
  5. **Order Status Breakdown** — Pivot on `order_status` to get counts per category. Return & Cancellation Rates
- Write each output to Parquet, partitioned and bucketed where appropriate.
- Use **caching** on the base DataFrame to speed up multiple downstream transformations.

---

### Module 4 — Airflow Orchestration (Week 4)

**Goal:** Schedule and manage the full pipeline.

- Create an Airflow DAG named `ecommerce_pipeline` in a file called `ecommerce_dag.py`.
- Define the following tasks with proper dependencies:

  ```
  start >> check_kafka_topic >> run_streaming_job >> wait_for_raw_data
        >> run_rdd_etl >> run_df_etl >> validate_output >> end
  ```

- **Task details:**

  | Task                 | Operator Type       | Description                                      |
  |----------------------|---------------------|--------------------------------------------------|
  | `start`              | DummyOperator       | Pipeline entry point                             |
  | `check_kafka_topic`  | PythonOperator      | Verify the Kafka topic exists and has messages   |
  | `run_streaming_job`  | BashOperator        | Submit the Spark Streaming job via `spark-submit` |
  | `wait_for_raw_data`  | FileSensor          | Wait until raw Parquet files appear              |
  | `run_rdd_etl`        | BashOperator        | Submit the RDD batch job                         |
  | `run_df_etl`         | BashOperator        | Submit the DataFrame batch job                   |
  | `validate_output`    | PythonOperator      | Check row counts & schema of output files        |
  | `end`                | DummyOperator       | Pipeline exit point                              |

- Configure:
  - `schedule_interval`: `@daily`
  - `retries`: 2, `retry_delay`: 5 minutes
  - `email_on_failure`: `true`
  - Use **Connections** for Kafka broker and Spark cluster settings.
  - Create at least one **parameterized DAG** that accepts `execution_date` as a parameter.

---

## Deliverables

| #  | Deliverable                        | Format              |
|----|------------------------------------|----------------------|
| 1  | `producer.py`                      | Python script        |
| 2  | `stream_consumer.py`               | PySpark script       |
| 3  | `batch_rdd_etl.py`                 | PySpark script       |
| 4  | `batch_df_etl.py`                  | PySpark script       |
| 5  | `ecommerce_dag.py`                 | Airflow DAG          |
| 6  | `regions.csv`                      | Reference data       |
| 7  | `README.md`                        | Setup & run guide    |
| 8  | Sample output screenshots          | PNG / Markdown       |

---

## Folder Structure

```
e-commerce-analytics-pipeline/
├── docker-compose.yml
├── Dockerfile.airflow
├── .env
├── README.md
├── data/
│   ├── regions.csv
│   ├── bronze/                  # Raw Parquet output from streaming
│   ├── silver/  
│   └── gold/                    # Aggregated Parquet output from batch ETL
└── src
    ├── kafka/
    │   └── create_topics.py
    │   └── producer.py
    ├── batch/
    │   ├── rdd_etl.py
    │   └── df_etl.py
    ├── streaming/
    │   └── stream_consumer.py
    ├── util/
    │   ├── deduplication.py
    │   └── logging.py
    └── airflow/
        └── dags/
           └── ecommerce_dag.py
```

---

## Evaluation Criteria

| Area                     | Weight | What We Look For                                              |
|--------------------------|:------:|---------------------------------------------------------------|
| Kafka Integration        | 20%    | Proper topic setup, message schema, producer reliability      |
| Spark Streaming          | 15%    | Correct consumption, deserialization, partitioned Parquet sink |
| RDD Processing           | 15%    | Use of transformations, key-value RDDs, accumulators          |
| DataFrame / Spark SQL    | 20%    | Aggregations, joins, window functions, caching, bucketing     |
| Airflow DAG              | 20%    | Task dependencies, operator usage, parameterization, retries  |
| Code Quality & Docs      | 10%    | Clean code, README, inline comments, reproducibility          |

---

## Stretch Goals (Optional)

- Deploy the Spark jobs on an **AWS EMR** cluster.
- Use **Spark accumulators** to track bad/malformed records during RDD processing.
- Add a second Kafka topic (`order_updates`) for status changes and join both streams.
- Implement **dynamic DAGs** in Airflow that auto-generate tasks based on a config file.
- Add data quality checks using assertions in the `validate_output` task.
