import narwhals as nw
import pandas as pd
import polars as pl

from laktory import get_spark_session
from laktory.enums import DataFrameBackends


def get_backend(v):
    if isinstance(v, str):
        return DataFrameBackends(v)
    return DataFrameBackends.from_nw_implementation(nw.from_native(v).implementation)


def to_backend(df, backend, lazy=False):
    backend = get_backend(backend)

    if backend == DataFrameBackends.POLARS:
        df = pl.from_pandas(df)
        if lazy:
            df = df.lazy()
    elif backend == DataFrameBackends.PYSPARK:
        spark = get_spark_session()
        df = spark.createDataFrame(df)
    return nw.from_native(df)


def get_df0(backend, lazy=False):
    df = pd.DataFrame(
        {
            "id": ["a", "b", "c"],
            "x1": [1, 2, 3],
        }
    )

    return to_backend(df, backend, lazy=lazy)


def get_df1(backend, lazy=False):
    df = pd.DataFrame(
        {
            "id": ["b", "c", "d"],
            "x2": [4, 9, 16],
        }
    )

    return to_backend(df, backend, lazy=lazy)
