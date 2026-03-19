import pytest
import logging
import tempfile
from pyspark.sql import SparkSession

from src.util.deduplication import deduplicate_events
from src.util.logging import get_logger

# Spark session for tests
@pytest.fixture(scope="session")
def spark():

    spark = SparkSession.builder.appName("test").getOrCreate()
    yield spark
    spark.stop()


class TestDeduplication:
    # Test duplicate event_ids are removed
    def test_deduplicate_events_removes_duplicates(self, spark):
        data = [
            {"event_id": "1", "timestamp": "2023-01-01 10:00:00"},
            {"event_id": "1", "timestamp": "2023-01-01 10:00:00"},
            {"event_id": "2", "timestamp": "2023-01-01 10:01:00"}
        ]
        df = spark.createDataFrame(data)
        result = deduplicate_events(df)
        assert result.count() == 2

    # Test rows with null timestamps are filtered out
    def test_deduplicate_events_filters_null_timestamps(self, spark):
        
        data = [
            {"event_id": "1", "timestamp": "2023-01-01 10:00:00"},
            {"event_id": "2", "timestamp": None}
        ]
        df = spark.createDataFrame(data)
        result = deduplicate_events(df)
        assert result.count() == 1

    # Test with no duplicates, all rows should remain
    def test_deduplicate_events_no_duplicates(self, spark):
        
        data = [
            {"event_id": "1", "timestamp": "2023-01-01 10:00:00"},
            {"event_id": "2", "timestamp": "2023-01-01 10:01:00"}
        ]
        df = spark.createDataFrame(data)
        result = deduplicate_events(df)
        assert result.count() == 2


class TestLogging:

    # Test get_logger returns a logger with the correct name
    def test_get_logger_returns_logger_with_correct_name(self):
        
        logger = get_logger("test_logger_name")
        assert logger.name == "test_logger_name"
        assert isinstance(logger, logging.Logger)

    # Test logger level is set correctly
    def test_get_logger_sets_correct_level(self):
        
        logger = get_logger("test_logger_level", level="DEBUG")
        assert logger.level == logging.DEBUG

    # Test console handler is added
    def test_get_logger_adds_console_handler(self):
        
        logger = get_logger("test_logger_console")
        handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(handlers) == 1

    # Test file handler is added when log_file is provided
    def test_get_logger_adds_file_handler_when_specified(self):
        
        with tempfile.NamedTemporaryFile() as temp_file:
            logger = get_logger("test_logger_file", log_file=temp_file.name)
            handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
            assert len(handlers) == 1
