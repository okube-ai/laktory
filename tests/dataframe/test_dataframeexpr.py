from __future__ import annotations

import polars as pl
import pytest

import laktory as lk
from laktory._testing import assert_dfs_equal
from laktory._testing import get_df0
from laktory._testing import get_df1


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_sql_expr(backend):
    df0 = get_df0(backend)

    node = lk.models.DataFrameExpr(expr="SELECT id, 3*x1 AS x3 FROM {df}")

    df = node.to_df({"df": df0})
    assert_dfs_equal(
        df.select("id", "x3"), pl.DataFrame({"id": ["a", "b", "c"], "x3": [3, 6, 9]})
    )


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_sql_expr_multi(backend):
    df0 = get_df0(backend)
    source = get_df1(backend)

    node = lk.models.DataFrameExpr(
        expr="SELECT * FROM {df} LEFT JOIN {source} on {df}.id = {source}.id"
    )
    df = node.to_df({"df": df0, "source": source})
    assert_dfs_equal(df.select("x2"), pl.DataFrame({"x2": [None, 4, 9]}))


def test_sql_with_nodes():
    e1 = lk.models.DataFrameExpr(expr="SELECT * FROM {df}")

    e2 = lk.models.DataFrameExpr(
        expr="SELECT * FROM {df} UNION SELECT * FROM {nodes.node_01} UNION SELECT * FROM {nodes.node_02}"
    )

    assert e1.data_sources == []
    assert e2.data_sources == [
        lk.models.PipelineNodeDataSource(node_name="node_01"),
        lk.models.PipelineNodeDataSource(node_name="node_02"),
    ]
