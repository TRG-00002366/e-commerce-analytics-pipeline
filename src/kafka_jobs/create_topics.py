"""
Script to create Kafka topics for the e-commerce analytics pipeline.
"""

import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Create Kafka topics
def create_topic():

    admin_client = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_SERVERS", "kafka: 29092"),
        client_id="topic_creator"
    )

    topic = NewTopic(name="order_events",
                     num_partitions=3,
                     replication_factor=1)

    try:
        admin_client.create_topics(
            new_topics=[topic],
            validate_only=False
        )
        print("Topic created successfully.")

    except TopicAlreadyExistsError:
        print("Topic already exists.")

    finally:
        admin_client.close()