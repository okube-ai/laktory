import inspect
import importlib
from functools import wraps
import pyspark.sql.session
from pyspark.sql.session import SparkSession

filepath = inspect.getabsfile(pyspark.sql.session)

spec = importlib.util.spec_from_file_location("spark0", filepath)
spark0 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(spark0)


class LaktorySparkSession:
    """
    Databricks DLT module overwrite the spark.sql method, preventing from using
    the latest parametrized queries. To circumvent this, we read the function
    definition from the disk and assign it to a laktory namespace so that it
    can be used in addition to the overwritten sql.
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    @wraps(spark0.SparkSession.sql)
    def sql(self, *args, **kwargs):
        return spark0.SparkSession.sql(self._spark, *args, **kwargs)


SparkSession.laktory: LaktorySparkSession = property(
    lambda self: LaktorySparkSession(self)
)
