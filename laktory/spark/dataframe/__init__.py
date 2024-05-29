from pyspark.sql.dataframe import DataFrame

from laktory.spark.dataframe.display import display
from laktory.spark.dataframe.groupby_and_agg import groupby_and_agg
from laktory.spark.dataframe.has_column import has_column
from laktory.spark.dataframe.schema_flat import schema_flat
from laktory.spark.dataframe.show_string import show_string
from laktory.spark.dataframe.smart_join import smart_join
from laktory.spark.dataframe.watermark import watermark
from laktory.spark.dataframe.window_filter import window_filter


# DataFrame Extensions
DataFrame.display = display
DataFrame.groupby_and_agg = groupby_and_agg
DataFrame.has_column = has_column
DataFrame.schema_flat = schema_flat
DataFrame.show_string = show_string
DataFrame.smart_join = smart_join
DataFrame.watermark = watermark
DataFrame.window_filter = window_filter

# Spark Connect DataFrame Extensions
try:
    from pyspark.sql.connect.dataframe import DataFrame

    DataFrame.display = display
    DataFrame.groupby_and_agg = groupby_and_agg
    DataFrame.has_column = has_column
    DataFrame.schema_flat = schema_flat
    DataFrame.show_string = show_string
    DataFrame.smart_join = smart_join
    DataFrame.watermark = watermark
    DataFrame.window_filter = window_filter
except:
    pass
