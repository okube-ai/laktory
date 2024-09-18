import inspect
import importlib
from functools import wraps
import pyspark.sql.connect.session
from pyspark.sql.connect.session import SparkSession

filepath = inspect.getabsfile(pyspark.sql.connect.session)

spec = importlib.util.spec_from_file_location("spark0", filepath)
spark0 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(spark0)


class LaktorySparkConnectSession:
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
        # import inspect
        # print(spark0)
        # print(inspect.getsource(spark0.SparkSession.sql))
        return spark0.SparkSession.sql(self._spark, *args, **kwargs)


SparkSession.laktory: LaktorySparkConnectSession = property(
    lambda self: LaktorySparkConnectSession(self)
)
