import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPICS = [
    NewTopic(
        name="order_events",
        num_partitions=3,
        replication_factor=1
    )
]


def create_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_SERVERS", "kafka: 29092"),
        client_id="topic_creator"
    )

    try:
        admin_client.create_topics(
            new_topics=TOPICS,
            validate_only=False
        )
        print("Topics created successfully.")

    except TopicAlreadyExistsError:
        print("One or more topics already exist.")

    finally:
        admin_client.close()


if __name__ == "__main__":
    create_topics()