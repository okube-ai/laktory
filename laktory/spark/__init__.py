spark_installed = True
try:
    import pyspark
except ModuleNotFoundError:
    spark_installed = False

if spark_installed:
    from pyspark.sql.connect.dataframe import DataFrame as SparkConnectDataFrame
    from pyspark.sql.dataframe import DataFrame as SparkDataFrame
    from pyspark.sql.column import Column as SparkColumn
    from pyspark.sql.session import SparkSession

    import laktory.spark.dataframe
    import laktory.spark.functions
    import laktory.spark.session
    import laktory.spark.connectsession
    from laktory.spark.datatypes import DATATYPES_MAP

    def is_spark_dataframe(df):
        """Check if dataframe is Spark DataFrame or Spark Connect DataFrame"""

        if isinstance(df, SparkConnectDataFrame):
            return True

        if isinstance(df, SparkDataFrame):
            return True

        return False

else:
    # Mocks when pyspark is not installed

    class SparkDataFrame:
        pass

    class SparkConnectDataFrame:
        pass

    class SparkColumn:
        pass

    class SparkSession:
        pass

    def is_spark_dataframe(df):
        return True
