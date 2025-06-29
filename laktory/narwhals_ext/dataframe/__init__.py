from functools import wraps

from laktory.narwhals_ext.dataframe.display import display
from laktory.narwhals_ext.dataframe.groupby_and_agg import groupby_and_agg
from laktory.narwhals_ext.dataframe.has_column import has_column
from laktory.narwhals_ext.dataframe.schema_flat import schema_flat
from laktory.narwhals_ext.dataframe.signature import signature

# from laktory.narwhals_ext.dataframe._stream_join import stream_join
from laktory.narwhals_ext.dataframe.union import union
from laktory.narwhals_ext.dataframe.window_filter import window_filter
from laktory.narwhals_ext.dataframe.with_row_index import with_row_index
from laktory.narwhals_ext.namespace import register_anyframe_namespace
from laktory.typing import AnyFrame


@register_anyframe_namespace("laktory")
class LaktoryDataFrame:  # noqa: F811
    def __init__(self, df: AnyFrame):
        self._df = df

    @wraps(display)
    def display(self, *args, **kwargs):
        return display(self, *args, **kwargs)

    @wraps(groupby_and_agg)
    def groupby_and_agg(self, *args, **kwargs):
        return groupby_and_agg(self, *args, **kwargs)

    @wraps(has_column)
    def has_column(self, *args, **kwargs):
        return has_column(self, *args, **kwargs)

    @wraps(schema_flat)
    def schema_flat(self, *args, **kwargs):
        return schema_flat(self, *args, **kwargs)

    @wraps(signature)
    def signature(self, *args, **kwargs):
        return signature(self, *args, **kwargs)

    # @wraps(stream_join)
    # def stream_join(self, *args, **kwargs):
    #     return stream_join(self, *args, **kwargs)

    @wraps(union)
    def union(self, *args, **kwargs):
        return union(self, *args, **kwargs)

    @wraps(with_row_index)
    def with_row_index(self, *args, **kwargs):
        return with_row_index(self, *args, **kwargs)

    @wraps(window_filter)
    def window_filter(self, *args, **kwargs):
        return window_filter(self, *args, **kwargs)
