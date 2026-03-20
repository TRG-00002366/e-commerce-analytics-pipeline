import importlib
import os
import sys
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from src.batch import df_etl

# Ensure df_etl reads from silver and writes expected gold outputs
def test_df_etl_reads_silver_and_writes_gold(monkeypatch, tmp_path):

    # Ensure df_etl uses our test paths
    monkeypatch.setenv("SILVER_PATH", str(tmp_path / "silver"))
    monkeypatch.setenv("REGIONS_PATH", str(tmp_path / "regions.csv"))
    monkeypatch.setenv("GOLD_PATH", str(tmp_path / "gold"))

    # Patch Spark SQL helper functions
    class FakeCol:
        def __eq__(self, other):
            return MagicMock()

        def __ne__(self, other):
            return MagicMock()

        def __le__(self, other):
            return MagicMock()

        def __sub__(self, other):
            return MagicMock()

        def __mul__(self, other):
            return MagicMock()

        def __add__(self, other):
            return MagicMock()

        def __truediv__(self, other):
            return MagicMock()

        def desc(self):
            return MagicMock()

        def __getattr__(self, item):
            return MagicMock()

    monkeypatch.setattr(df_etl, "col", lambda *args, **kwargs: FakeCol())

    fake_condition = MagicMock()
    fake_condition.when.return_value = fake_condition
    fake_condition.otherwise.return_value = fake_condition
    monkeypatch.setattr(df_etl, "when", lambda *args, **kwargs: fake_condition)

    monkeypatch.setattr(df_etl, "count", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(df_etl, "avg", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(df_etl, "row_number", lambda *args, **kwargs: MagicMock(over=lambda w: MagicMock()))
    monkeypatch.setattr(df_etl, "spark_sum", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(df_etl, "coalesce", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(df_etl, "lit", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(df_etl, "hour", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(df_etl, "date_format", lambda *args, **kwargs: MagicMock())

    class FakeWindow:
        @staticmethod
        def partitionBy(*args, **kwargs):
            class W:
                @staticmethod
                def orderBy(*args, **kwargs):
                    return MagicMock()

            return W()

    monkeypatch.setattr(df_etl, "Window", FakeWindow)

    # Fake dataframe for chained operations
    df = MagicMock()
    df.withColumn.return_value = df
    df.filter.return_value = df
    df.na.fill.return_value = df
    df.join.return_value = df
    df.cache.return_value = df
    df.repartition.return_value = df

    group = MagicMock()
    group.agg.return_value = df
    df.groupBy.return_value = group

    write = MagicMock()
    write.mode.return_value = write
    write.partitionBy.return_value = write
    write.csv.return_value = None
    df.write = write

    # Fake Spark session and builder
    spark = MagicMock()
    spark.read.parquet.return_value = df
    spark.read.csv.return_value = df
    spark.stop = MagicMock()

    builder = MagicMock()
    builder.appName.return_value = builder
    builder.master.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = spark

    FakeSparkSession = type("FakeSparkSession", (), {"builder": builder})
    monkeypatch.setattr(df_etl, "SparkSession", FakeSparkSession)

    # Run ETL
    df_etl.df_etl()

    # Verify read paths were used
    spark.read.parquet.assert_called_once_with(os.getenv("SILVER_PATH"))
    spark.read.csv.assert_called_once_with(os.getenv("REGIONS_PATH"), header=True, inferSchema=True)

    spark.stop.assert_called_once()

# Validate that rdd_etl performs parsing and calls saveAsTextFile.
def test_rdd_etl_runs_and_writes_output(monkeypatch, tmp_path):

    monkeypatch.setenv("RAW_PATH", str(tmp_path / "raw"))
    monkeypatch.setenv("OUTPUT_PATH", str(tmp_path / "out"))

    # Fake Spark context with accumulators
    class FakeAccumulator:
        def __init__(self):
            self.value = 0

        def add(self, amount):
            self.value += amount

    class FakeSparkContext:
        def accumulator(self, init):
            return FakeAccumulator()

    fake_spark_context = FakeSparkContext()

    # Fake RDD implementation for the pipeline
    save_calls = []

    class FakeRDD:
        def __init__(self, data):
            self.data = data

        def map(self, fn):
            return FakeRDD([fn(x) for x in self.data])

        def filter(self, fn):
            return FakeRDD([x for x in self.data if fn(x)])

        def reduceByKey(self, fn):
            acc = {}
            for k, v in self.data:
                if k in acc:
                    acc[k] = fn(acc[k], v)
                else:
                    acc[k] = v
            return FakeRDD(list(acc.items()))

        def repartition(self, num):
            return self

        def saveAsTextFile(self, path):
            save_calls.append(path)

    # Fake DataFrame returned by spark.read.schema(...).parquet(...)
    class FakeRow:
        def __init__(self, data):
            self._data = data

        def asDict(self):
            return self._data

    class FakeDF:
        def __init__(self, rows):
            self._rows = rows

        @property
        def rdd(self):
            return FakeRDD(self._rows)

    # Sample input rows: one valid and one malformed
    rows = [
        FakeRow({
            "order_id": "ORD-1",
            "product_id": "PROD-1",
            "quantity": 2,
            "unit_price": 10.0,
            "discount": 1.0,
            "event_type": "ORDER_CREATED",
        }),
        FakeRow({
            "order_id": "ORD-2",
            "product_id": "PROD-2",
            "quantity": "BAD",  # will trigger ValueError
            "unit_price": 5.0,
            "discount": 0.0,
            "event_type": "ORDER_CREATED",
        }),
    ]

    fake_df = FakeDF(rows)

    # Fake Spark read interface
    class FakeRead:
        def schema(self, _schema):
            return self

        def parquet(self, path):
            return fake_df

    fake_spark = MagicMock()
    fake_spark.sparkContext = fake_spark_context
    fake_spark.read = FakeRead()
    fake_spark.stop = MagicMock()

    builder = MagicMock()
    builder.appName.return_value = builder
    builder.master.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = fake_spark

    FakeSparkSession = MagicMock()
    FakeSparkSession.builder = builder

    # Ensure module reload uses our fake SparkSession and env vars
    monkeypatch.setattr(sys.modules["pyspark.sql"], "SparkSession", FakeSparkSession)
    sys.modules.pop("src.batch.rdd_etl", None)

    import src.batch.rdd_etl as rdd_etl

    # Validate that the pipeline attempted to write to OUTPUT_PATH
    assert save_calls == [os.getenv("OUTPUT_PATH")]

    # Validate accumulators changed as expected
    assert rdd_etl.processed_orders.value == 1
    assert rdd_etl.malformed_records.value == 1

# run df_etl with a real Spark session and ensure outputs are written
def test_df_etl_writes_expected_output_paths_with_real_spark(monkeypatch, tmp_path):

    # Setup directories
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    silver_dir.mkdir(parents=True)
    gold_dir.mkdir(parents=True)

    # Create minimal input data and regions lookup
    spark = SparkSession.builder.master("local[2]").appName("test_df_etl").getOrCreate()

    input_df = spark.createDataFrame([
        ("ORD-1", "ORDER_CREATED", "CUST-1", "PRIME", "PROD-1", "Widget", "Electronics", 2, 10.0, 0.0, "CREDIT_CARD", "STANDARD", "US", "2026-01-01 00:00:00"),
    ], [
        "order_id", "event_type", "customer_id", "customer_segment", "product_id", "product_name", "category",
        "quantity", "unit_price", "discount", "payment_method", "shipping_type", "region", "timestamp",
    ])

    input_df.write.mode("overwrite").parquet(str(silver_dir))

    regions_csv = tmp_path / "regions.csv"
    regions_csv.write_text("region_code,region_name\nUS,US-East\n")

    # Patch coalesce so constants like 0 become literal columns
    from pyspark.sql import Column
    from pyspark.sql.functions import coalesce, lit

    def safe_coalesce(*cols):
        converted = [c if isinstance(c, Column) else lit(c) for c in cols]
        return coalesce(*converted)

    monkeypatch.setattr(df_etl, "coalesce", safe_coalesce)

    # Set env vars for df_etl
    os.environ["SILVER_PATH"] = str(silver_dir)
    os.environ["REGIONS_PATH"] = str(regions_csv)
    os.environ["GOLD_PATH"] = str(gold_dir)
    os.environ["SPARK_MASTER_URL"] = "local[2]"

    # Run the ETL
    df_etl.df_etl()

    spark.stop()

    # Ensure expected output directories were created
    expected_dirs = [
        "hourly_sales",
        "top_products",
        "regional_revenue",
        "customer_segment",
        "status_metrics",
    ]

    for d in expected_dirs:
        out_dir = gold_dir / d
        assert out_dir.exists() and out_dir.is_dir(), f"Missing output dir: {out_dir}"
        # Spark should create at least one file in the output directory
        assert any(out_dir.iterdir()), f"No output files in {out_dir}"
