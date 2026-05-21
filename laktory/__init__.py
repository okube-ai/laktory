from ._version import VERSION

__version__ = VERSION

# Import first
from ._settings import settings
from ._useragent import set_databricks_sdk_upstream

set_databricks_sdk_upstream()

import laktory._parsers
import laktory.api
import laktory.enums
import laktory.models
import laktory.narwhals_ext
import laktory.typing
import laktory.yaml

from ._cache import cache_dir
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

        spark = SparkSession.getActiveSession()

    if spark is None:
        # No active session — build one (local / test usage).
        # Requires pyspark pip metadata to determine Scala / Delta JAR versions.
        # In managed environments (DLT, Databricks Connect) a session is always
        # active, so this branch is never reached there.
        from importlib.metadata import version as pkg_version

        from pyspark.sql import SparkSession

        pyspark_ver = pkg_version("pyspark")
        spark_major = int(pyspark_ver.split(".")[0])
        scala = "2.13" if spark_major >= 4 else "2.12"

        # delta-spark Python package versions and Maven JAR versions are not 1:1.
        # Only delta-spark_2.13:4.0.0 exists on Maven Central for Spark 4.x.
        # For Spark 3.x the Python version matches the Maven JAR version.
        _delta_jvm = {"4": "4.0.0"}
        delta_jvm_ver = _delta_jvm.get(str(spark_major), pkg_version("delta_spark"))

        spark = (
            SparkSession.builder.appName("laktory")
            .config(
                "spark.jars.packages",
                f"org.apache.spark:spark-avro_{scala}:{pyspark_ver},"
                f"io.delta:delta-spark_{scala}:{delta_jvm_ver}",
            )
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


def is_ldp_execute() -> bool:
    from pyspark.errors import AnalysisException

    spark = get_spark_session()

    try:
        is_ldp = spark.conf.get("pipelines.dbrVersion", "na") != "na"
    except AnalysisException:
        # Default value is not supported on earlier versions of serverless
        is_ldp = False

    return is_ldp


def is_sdp_execute() -> bool:
    from pyspark.errors import AnalysisException

    spark = get_spark_session()

    try:
        has_flow = spark.conf.get("spark.pipelines.flow.name", "na") != "na"
        has_dbr = spark.conf.get("pipelines.dbrVersion", "na") != "na"
        has_flag = spark.conf.get("laktory.is_sdp_execute", "false") == "true"
        return (has_flow and not has_dbr) or has_flag
    except AnalysisException:
        return False


def print_version():
    print(show_version_info())
