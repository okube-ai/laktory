try:
    import pyspark

    import laktory.spark.dataframe
    import laktory.spark.functions

    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.column import Column

except ModuleNotFoundError:
    # Mocks when pyspark is not installed

    class DataFrame:
        pass

    class Column:
        pass
