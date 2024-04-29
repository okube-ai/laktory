try:
    import pyspark

    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.column import Column

    import laktory.spark.dataframe
    import laktory.spark.functions


except ModuleNotFoundError:
    # Mocks when pyspark is not installed

    class DataFrame:
        pass

    class Column:
        pass
