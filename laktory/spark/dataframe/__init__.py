from pyspark.sql.dataframe import DataFrame

from laktory.spark.dataframe.schema_flat import schema_flat
from laktory.spark.dataframe.has_column import has_column


# DataFrame Extensions
DataFrame.schema_flat = schema_flat
DataFrame.has_column = has_column

# Spark Connect DataFrame Extensions
try:
    from pyspark.sql.connect.dataframe import DataFrame

    DataFrame.schema_flat = schema_flat
    DataFrame.has_column = has_column
except:
    pass
