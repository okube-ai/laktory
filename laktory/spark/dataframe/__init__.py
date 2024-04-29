from pyspark.sql.dataframe import DataFrame

from laktory.spark.dataframe.schema_flat import schema_flat
from laktory.spark.dataframe.has_column import has_column
from laktory.spark.dataframe.show_string import show_string
from laktory.spark.dataframe.laktory_join import laktory_join
from laktory.spark.dataframe.watermark import watermark
from laktory.spark.dataframe.groupbyandagg import groupby_and_agg


# DataFrame Extensions
DataFrame.schema_flat = schema_flat
DataFrame.has_column = has_column
DataFrame.show_string = show_string
DataFrame.laktory_join = laktory_join
DataFrame.watermark = watermark
DataFrame.groupby_and_agg = groupby_and_agg

# Spark Connect DataFrame Extensions
try:
    from pyspark.sql.connect.dataframe import DataFrame

    DataFrame.schema_flat = schema_flat
    DataFrame.has_column = has_column
    DataFrame.show_string = show_string
    DataFrame.laktory_join = laktory_join
    DataFrame.watermark = watermark
    DataFrame.groupby_and_agg = groupby_and_agg
except:
    pass
