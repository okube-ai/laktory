from ._version import VERSION

__version__ = VERSION

# Import first
from ._settings import settings
from ._useragent import set_databricks_sdk_upstream

set_databricks_sdk_upstream()

import laktory._parsers
import laktory.api
import laktory.models
import laktory.narwhals_ext
import laktory.typing
import laktory.yaml

from ._logger import get_logger
from ._settings import Settings
from .sqlparser import SQLParser
from .version import show_version_info


def register_spark_session(spark=None):
    """Register a Spark session"""
    import sys

    _laktory = sys.modules[__name__]
    if spark is None:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("laktory")
            # TODO: Check if we can install on a need-basis
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-avro_2.12:3.5.5,io.delta:delta-spark_2.12:3.3.0",
            )  # com.databricks:spark-xml_2.12:0.17.0
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )
    _laktory._spark = spark


def get_spark_session():
    import sys

    _laktory = sys.modules[__name__]
    if not hasattr(_laktory, "_spark"):
        register_spark_session()
    return _laktory._spark
