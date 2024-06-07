from functools import wraps
import polars as pl

from laktory.polars.dataframe.groupby_and_agg import groupby_and_agg
from laktory.polars.dataframe.has_column import has_column
from laktory.polars.dataframe.schema_flat import schema_flat
from laktory.polars.dataframe.union import union


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

    @wraps(schema_flat)
    def schema_flat(self, *args, **kwargs):
        return schema_flat(self._df, *args, **kwargs)

    @wraps(union)
    def union(self, *args, **kwargs):
        return union(self._df, *args, **kwargs)
