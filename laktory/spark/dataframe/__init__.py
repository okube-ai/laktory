from pyspark.sql.dataframe import DataFrame

from laktory.spark.dataframe.schema_flat import schema_flat
from laktory.spark.dataframe.has_column import has_column
from laktory.spark.dataframe.show_string import show_string


# DataFrame Extensions
DataFrame.schema_flat = schema_flat
DataFrame.has_column = has_column
DataFrame.show_string = show_string

# Spark Connect DataFrame Extensions
try:
    from pyspark.sql.connect.dataframe import DataFrame

    DataFrame.schema_flat = schema_flat
    DataFrame.has_column = has_column
    DataFrame.show_string = show_string
except:
    pass
