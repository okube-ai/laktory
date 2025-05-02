import narwhals as nw
import pandas as pd
import polars as pl
import pytest

import laktory
from laktory.enums import DataFrameBackends
from laktory.models import DataFrameMethod
from laktory.models import DataFrameSQLExpr
from laktory.models import DataFrameTransformer


def get_backend(v):
    if isinstance(v, str):
        return DataFrameBackends(v)
    return DataFrameBackends.from_nw_implementation(nw.from_native(v).implementation)


def to_backend(df, backend):
    backend = get_backend(backend)

    if backend == DataFrameBackends.POLARS:
        df = pl.from_pandas(df)
    elif backend == DataFrameBackends.PYSPARK:
        spark = laktory.get_spark_session()
        df = spark.createDataFrame(df)
    return nw.from_native(df)


@pytest.fixture(params=["POLARS", "PYSPARK"])
def df0(request):
    df = pd.DataFrame(
        {
            "id": ["a", "b", "c"],
            "x1": [1, 2, 3],
        }
    )

    return to_backend(df, request.param)


def test_transformer(df0):
    node0 = DataFrameMethod(
        name="with_columns",
        kwargs={
            "y1": "x1",
        },
    )

    node1 = DataFrameSQLExpr(
        sql_expr="select id, x1, y1 from df",
    )

    transformer = DataFrameTransformer(nodes=[node0, node1])

    df = transformer.execute(df0)

    assert df.columns == ["id", "x1", "y1"]
