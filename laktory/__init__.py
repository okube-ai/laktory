from ._version import VERSION

__version__ = VERSION

# Import first
from ._settings import settings
from ._useragent import set_databricks_sdk_upstream

set_databricks_sdk_upstream()

import laktory._parsers
import laktory.models
import laktory.narwhals
import laktory.spark
import laktory.typing
import laktory.yaml

from ._logger import get_logger
from ._settings import Settings
from .version import show_version_info


def register_spark_session(spark=None):
    """Register a Spark session"""
    import sys

    _laktory = sys.modules[__name__]
    if spark is None:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("laktory")
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.5")
            # .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .getOrCreate()
        )
    _laktory._spark = spark


def get_spark_session():
    import sys

    _laktory = sys.modules[__name__]
    if not hasattr(_laktory, "_spark"):
        register_spark_session()
    return _laktory._spark
