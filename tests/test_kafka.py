import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


kafka_mod = types.ModuleType("kafka")
admin_mod = types.ModuleType("kafka.admin")
errors_mod = types.ModuleType("kafka.errors")

# Dummy classes used by the repository code.
admin_mod.KafkaAdminClient = MagicMock
admin_mod.NewTopic = MagicMock

# Producer uses kafka.KafkaProducer
kafka_mod.KafkaProducer = MagicMock

class TopicAlreadyExistsError(Exception):
    pass

errors_mod.TopicAlreadyExistsError = TopicAlreadyExistsError

sys.modules["kafka"] = kafka_mod
sys.modules["kafka.admin"] = admin_mod
sys.modules["kafka.errors"] = errors_mod

from src.kafka_jobs import create_topics, producer

# Ensure create_topics creates the configured topics and closes the client
def test_create_topics_calls_kafka_admin_and_closes():

    with patch("src.kafka.create_topics.KafkaAdminClient") as MockAdmin:
        admin_instance = MockAdmin.return_value

        env = {"KAFKA_SERVERS": "kafka:1234"}
        with patch.dict(os.environ, env, clear=True):
            create_topics.create_topics()

        MockAdmin.assert_called_once_with(
            bootstrap_servers=env["KAFKA_SERVERS"],
            client_id="topic_creator",
        )
        admin_instance.create_topics.assert_called_once_with(
            new_topics=create_topics.TOPICS,
            validate_only=False,
        )
        admin_instance.close.assert_called_once()

# If an existing topic error is raised, create_topics should still close the client
def test_create_topics_handles_topic_already_exists_error():

    with patch("src.kafka.create_topics.KafkaAdminClient") as MockAdmin:
        admin_instance = MockAdmin.return_value
        admin_instance.create_topics.side_effect = create_topics.TopicAlreadyExistsError()

        create_topics.create_topics()

        admin_instance.close.assert_called_once()

# generate_event should return a dict containing required fields
def test_generate_event_returns_expected_structure():

    event = producer.generate_event()

    assert isinstance(event, dict)
    assert event["event_type"] in producer.EVENT_TYPES
    assert isinstance(event["event_id"], str)
    assert isinstance(event["order_id"], str)
    assert isinstance(event["timestamp"], str)

# Ensure create_topics defines the expected topic name/partitions
def test_create_topics_uses_correct_newtopic_definition(monkeypatch):

    created = []

    class NewTopicStub:
        def __init__(self, name, num_partitions, replication_factor):
            self.name = name
            self.num_partitions = num_partitions
            self.replication_factor = replication_factor
            created.append(self)

    monkeypatch.setattr(admin_mod, "NewTopic", NewTopicStub)

    # Reload module so TOPICS is rebuilt using our stub
    sys.modules.pop("src.kafka.create_topics", None)
    import importlib

    create_topics_mod = importlib.import_module("src.kafka.create_topics")

    assert len(created) == 1
    topic = created[0]
    assert topic.name == "order_events"
    assert topic.num_partitions == 3
    assert topic.replication_factor == 1


# stream_events should call producer.send once before stopping due to KeyboardInterrupt
def test_stream_events_sends_at_least_one_event_and_exits(monkeypatch):

    send_mock = MagicMock()
    monkeypatch.setattr(producer, "producer", MagicMock(send=send_mock))

    # Stop the loop after the first sleep call
    def raise_keyboard_interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(producer.time, "sleep", raise_keyboard_interrupt)

    with pytest.raises(KeyboardInterrupt):
        producer.stream_events()

    send_mock.assert_called_once()
    call_args, call_kwargs = send_mock.call_args

    # verify positional topic argument
    assert call_args[0] == "order_events"

    # verify key + value passed as kwargs
    assert "key" in call_kwargs
    assert "value" in call_kwargs
    assert isinstance(call_kwargs["key"], (bytes, bytearray))
    assert isinstance(call_kwargs["value"], dict)
