# producer
docker exec -it airflow-scheduler python3 /opt/spark-apps/kafka_jobs/producer.py


# consumer
docker exec -u root -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/streaming/stream_consumer.py