import pytest

from laktory._testing import get_df0
from laktory.models import DataFrameExpr
from laktory.models import DataFrameMethod
from laktory.models import DataFrameTransformer


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_transformer(backend):
    df0 = get_df0(backend)

    node0 = DataFrameMethod(
        func_name="with_columns",
        func_kwargs={
            "y1": "x1",
        },
    )

    node1 = DataFrameExpr(
        expr="select id, x1, y1 from {df}",
    )

    transformer = DataFrameTransformer(nodes=[node0, node1])

    df = transformer.execute(df0)

    assert df.columns == ["id", "x1", "y1"]
    assert transformer.data_sources == []
