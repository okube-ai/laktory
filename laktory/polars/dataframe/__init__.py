from functools import wraps
import polars as pl

from laktory.polars.dataframe.schema_flat import schema_flat
from laktory.polars.dataframe.has_column import has_column
from laktory.polars.dataframe.union import union


@pl.api.register_dataframe_namespace("laktory")
class LaktoryDataFrame:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    @wraps(has_column)
    def has_column(self, *args, **kwargs):
        return has_column(self._df, *args, **kwargs)

    @wraps(schema_flat)
    def schema_flat(self, *args, **kwargs):
        return schema_flat(self._df, *args, **kwargs)

    @wraps(union)
    def union(self, *args, **kwargs):
        return union(self._df, *args, **kwargs)
