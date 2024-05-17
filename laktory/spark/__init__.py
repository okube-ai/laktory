try:
    import pyspark

    from pyspark.sql.connect.dataframe import DataFrame as SparkConnectDataFrame
    from pyspark.sql.dataframe import DataFrame as SparkDataFrame
    from pyspark.sql.column import Column as SparkColumn
    from pyspark.sql.session import SparkSession

    import laktory.spark.dataframe
    import laktory.spark.functions

    def is_spark_dataframe(df):
        """Check if dataframe is Spark DataFrame or Spark Connect DataFrame"""

        if isinstance(df, SparkConnectDataFrame):
            return True

        if isinstance(df, SparkDataFrame):
            return True

        return False

except ModuleNotFoundError:
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
