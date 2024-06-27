from functools import wraps
from pyspark.sql.dataframe import DataFrame

from laktory.spark.dataframe.display import display
from laktory.spark.dataframe.groupby_and_agg import groupby_and_agg
from laktory.spark.dataframe.has_column import has_column
from laktory.spark.dataframe.schema_flat import schema_flat
from laktory.spark.dataframe.show_string import show_string
from laktory.spark.dataframe.smart_join import smart_join
from laktory.spark.dataframe.watermark import watermark
from laktory.spark.dataframe.window_filter import window_filter


class LaktoryDataFrame:
    def __init__(self, df: DataFrame):
        self._df = df

    @wraps(display)
    def display(self, *args, **kwargs):
        return display(self._df, *args, **kwargs)

    @wraps(groupby_and_agg)
    def groupby_and_agg(self, *args, **kwargs):
        return groupby_and_agg(self._df, *args, **kwargs)

    @wraps(has_column)
    def has_column(self, *args, **kwargs):
        return has_column(self._df, *args, **kwargs)

    @wraps(schema_flat)
    def schema_flat(self, *args, **kwargs):
        return schema_flat(self._df, *args, **kwargs)

    @wraps(show_string)
    def show_string(self, *args, **kwargs):
        return show_string(self._df, *args, **kwargs)

    @wraps(smart_join)
    def smart_join(self, *args, **kwargs):
        return smart_join(self._df, *args, **kwargs)

    @wraps(watermark)
    def watermark(self, *args, **kwargs):
        return watermark(self._df, *args, **kwargs)

    @wraps(window_filter)
    def window_filter(self, *args, **kwargs):
        return window_filter(self._df, *args, **kwargs)


DataFrame.laktory: LaktoryDataFrame = property(lambda self: LaktoryDataFrame(self))


# Spark Connect DataFrame Extensions
try:
    from pyspark.sql.connect.dataframe import DataFrame

    DataFrame.laktory = property(lambda self: LaktoryDataFrame(self))
except:
    pass
