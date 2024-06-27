from pyspark.sql.session import SparkSession

_sql = SparkSession.sql


class LaktorySparkSession:
    """
    Databricks DLT module overwrite the spark.sql method, preventing from using
    the latest parametrized queries. To circumvent this, we copy the baseline
    sql function into a laktory namespace so that it can be used in addition to
    the overwritten sql.
    """
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def sql(self, *args, **kwargs):
        return _sql(self._spark, *args, **kwargs)

SparkSession.laktory: LaktorySparkSession = property(lambda self: LaktorySparkSession(self))
