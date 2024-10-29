from functools import wraps
import polars as pl

from laktory.polars.dataframe.groupby_and_agg import groupby_and_agg
from laktory.polars.dataframe.has_column import has_column
from laktory.polars.dataframe.schema_flat import schema_flat
from laktory.polars.dataframe.signature import signature
from laktory.polars.dataframe.smart_join import smart_join
from laktory.polars.dataframe.union import union
from laktory.polars.dataframe.window_filter import window_filter


@pl.api.register_dataframe_namespace("laktory")
class LaktoryDataFrame:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    @wraps(groupby_and_agg)
    def groupby_and_agg(self, *args, **kwargs):
        return groupby_and_agg(self._df, *args, **kwargs)

    @wraps(has_column)
    def has_column(self, *args, **kwargs):
        return has_column(self._df, *args, **kwargs)

    @wraps(signature)
    def signature(self, *args, **kwargs):
        return signature(self._df, *args, **kwargs)

    @wraps(schema_flat)
    def schema_flat(self, *args, **kwargs):
        return schema_flat(self._df, *args, **kwargs)

    @wraps(smart_join)
    def smart_join(self, *args, **kwargs):
        return smart_join(self._df, *args, **kwargs)

    @wraps(union)
    def union(self, *args, **kwargs):
        return union(self._df, *args, **kwargs)

    @wraps(window_filter)
    def window_filter(self, *args, **kwargs):
        return window_filter(self._df, *args, **kwargs)


@pl.api.register_lazyframe_namespace("laktory")
class LaktoryDataFrame:
    def __init__(self, df: pl.LazyFrame):
        self._df = df

    @wraps(groupby_and_agg)
    def groupby_and_agg(self, *args, **kwargs):
        return groupby_and_agg(self._df, *args, **kwargs)

    @wraps(has_column)
    def has_column(self, *args, **kwargs):
        return has_column(self._df, *args, **kwargs)

    @wraps(signature)
    def signature(self, *args, **kwargs):
        return signature(self._df, *args, **kwargs)

    @wraps(schema_flat)
    def schema_flat(self, *args, **kwargs):
        return schema_flat(self._df, *args, **kwargs)

    @wraps(smart_join)
    def smart_join(self, *args, **kwargs):
        return smart_join(self._df, *args, **kwargs)

    @wraps(union)
    def union(self, *args, **kwargs):
        return union(self._df, *args, **kwargs)

    @wraps(window_filter)
    def window_filter(self, *args, **kwargs):
        return window_filter(self._df, *args, **kwargs)
