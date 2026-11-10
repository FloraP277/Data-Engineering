import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

USE_DELTA = os.environ.get("USE_DELTA") == "1"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder.master("local[2]").appName("tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.showConsoleProgress", "false")
               .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse"))))
    if USE_DELTA:
        # Used in CI, where the Delta Lake jars can be downloaded.
        from delta import configure_spark_with_delta_pip
        builder = (builder
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def store(spark, tmp_path):
    from lakehouse.store import DeltaStore, ParquetStore

    if USE_DELTA:
        schema = f"t_{abs(hash(str(tmp_path))) % 10**8}"
        return DeltaStore(spark, "spark_catalog", schema)
    return ParquetStore(spark, str(tmp_path / "tables"))
