import os
import pytest
from pyspark.sql import SparkSession

def pytest_configure():
    os.environ.setdefault('APP_ENV', 'test')

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("pytest-spark")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    yield spark
    spark.stop()
