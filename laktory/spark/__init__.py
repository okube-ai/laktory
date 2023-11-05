try:
    import pyspark

    import laktory.spark.dataframe
    import laktory.spark.functions

    from pyspark.sql.dataframe import DataFrame

except ModuleNotFoundError:

    class DataFrame:
        pass
