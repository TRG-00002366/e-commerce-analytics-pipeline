# Use the official Airflow image as a base
FROM apache/airflow:2.7.1

USER root

# 1. Install Java (Required for PySpark/spark-submit)
# Changed 'jr' to 'jre' for the correct package name
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Set JAVA_HOME so Spark can find it
# Docker handles the export automatically with 'ENV'
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# 3. Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt