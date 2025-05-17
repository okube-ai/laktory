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
            "_idx": [0, 1, 2],
            "id": ["a", "b", "c"],
            "x1": [1, 2, 3],
        }
    )

    return to_backend(df, backend, lazy=lazy)


def get_df1(backend, lazy=False):
    df = pd.DataFrame(
        {
            "_idx": [0, 1, 2],
            "id": ["b", "c", "d"],
            "x2": [4, 9, 16],
        }
    )

    return to_backend(df, backend, lazy=lazy)


class StreamingSource:
    def __init__(
        self,
        backend="POLARS",
    ):
        self.backend = backend
        self.ibatch = 0
        self.irow = -1

    def get_batch_df(self, nbatch=1):
        dfs = []
        for _ibatch in range(nbatch):
            df = get_df0(self.backend, lazy=True)
            df = df.with_columns(_batch_id=nw.lit(self.ibatch))
            df = df.with_columns(_idx=nw.col("_idx") + self.ibatch * 3)
            dfs += [df]
            self.ibatch += 1

        if nbatch == 1:
            return dfs[0]

        df = dfs[0].to_native()
        for _df in dfs[1:]:
            if self.backend == "POLARS":
                df = df.concat(_df.to_native())
            elif self.backend == "PYSPARK":
                df = df.union(_df.to_native())

        return nw.from_native(df)

    def write_to_delta(self, filepath, nbatch=1):
        is_init = self.ibatch == 0

        df = self.get_batch_df(nbatch)

        if self.backend == "PYSPARK":
            if is_init:
                df.to_native().write.format("DELTA").mode("overwrite").save(filepath)
            else:
                df.to_native().write.format("DELTA").mode("append").save(filepath)
        else:
            raise NotImplementedError()
