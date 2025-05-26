from functools import wraps

import narwhals as nw

from laktory.narwhals.dataframe.groupby_and_agg import groupby_and_agg
from laktory.narwhals.dataframe.schema_flat import schema_flat
from laktory.typing import AnyFrame


class LaktoryDataFrame:  # noqa: F811
    def __init__(self, df: AnyFrame):
        self._df = df

    @wraps(groupby_and_agg)
    def groupby_and_agg(self, *args, **kwargs):
        return groupby_and_agg(self._df, *args, **kwargs)

    # @wraps(has_column)
    # def has_column(self, *args, **kwargs):
    #     return has_column(self._df, *args, **kwargs)
    #
    # @wraps(signature)
    # def signature(self, *args, **kwargs):
    #     return signature(self._df, *args, **kwargs)
    #
    @wraps(schema_flat)
    def schema_flat(self, *args, **kwargs):
        return schema_flat(self._df, *args, **kwargs)

    # @wraps(smart_join)
    # def smart_join(self, *args, **kwargs):
    #     return smart_join(self._df, *args, **kwargs)
    #
    # @wraps(union)
    # def union(self, *args, **kwargs):
    #     return union(self._df, *args, **kwargs)
    #
    # @wraps(window_filter)
    # def window_filter(self, *args, **kwargs):
    #     return window_filter(self._df, *args, **kwargs)


nw.DataFrame.laktory: LaktoryDataFrame = property(lambda self: LaktoryDataFrame(self))
nw.LazyFrame.laktory: LaktoryDataFrame = property(lambda self: LaktoryDataFrame(self))
