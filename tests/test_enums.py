from __future__ import annotations

import narwhals as nw
import polars as pl

from laktory.enums import DataFrameBackends


def test_dataframe_backends():
    df_pl = pl.DataFrame()
    df_nw = nw.from_native(df_pl)

    assert DataFrameBackends("POLARS") == DataFrameBackends.POLARS
    assert (
        DataFrameBackends.from_nw_implementation(df_nw.implementation)
        == DataFrameBackends.POLARS
    )
    assert DataFrameBackends.from_df(df_pl) == DataFrameBackends.POLARS
    assert DataFrameBackends.from_df(df_nw) == DataFrameBackends.POLARS
