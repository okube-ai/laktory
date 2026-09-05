import narwhals as nw
import polars as pl
import pytest
from pydantic import ValidationError

import laktory as lk
from laktory._testing import get_df0
from laktory._testing import get_df1

from ..conftest import assert_dfs_equal


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
    assert [s.node_name for s in e2.data_sources] == ["node_01", "node_02"]

    assert e1.upstream_node_names == []
    assert e2.upstream_node_names == ["node_01", "node_02"]


@pytest.mark.parametrize("backend", ["PYSPARK"])
def test_sql_with_curly(backend):
    df0 = get_df0(backend)

    df0 = df0.with_columns(filename=nw.lit("file_20250826.csv"))

    node = lk.models.DataFrameExpr(
        expr="SELECT regexp_extract(filename, 'file_([0-9]{8,8})', 1) AS date FROM {df}"
    )

    df = node.to_df({"df": df0})
    df.to_native().show()
    assert_dfs_equal(
        df.select("date"),
        pl.DataFrame(
            {
                "date": [
                    "20250826",
                ]
                * 3
            }
        ),
    )


# ---------------------------------------------------------------------------
# Single-statement enforcement (issue #640)
# ---------------------------------------------------------------------------

_COMMENT_SEMICOLON_EXPR = """
SELECT
    id,
    -- TODO: rule isn't specified in the sheet's notes; passthrough for now
    x1
FROM
    {df}
"""


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_sql_expr_comment_with_semicolon(backend):
    """A `;` inside a `-- comment` is not mistaken for a statement separator."""
    df0 = get_df0(backend)

    node = lk.models.DataFrameExpr(expr=_COMMENT_SEMICOLON_EXPR)
    df = node.to_df({"df": df0})
    assert_dfs_equal(
        df.select("id", "x1"),
        pl.DataFrame({"id": ["a", "b", "c"], "x1": [1, 2, 3]}),
    )


def test_sql_expr_comment_with_semicolon_sdp(monkeypatch):
    """Same as above, but through the Spark Connect / SDP execution branch."""
    monkeypatch.setattr(lk, "is_sdp_execute", lambda: True)
    df0 = get_df0("PYSPARK")

    node = lk.models.DataFrameExpr(expr=_COMMENT_SEMICOLON_EXPR)
    df = node.to_df({"df": df0})
    assert_dfs_equal(
        df.select("id", "x1"),
        pl.DataFrame({"id": ["a", "b", "c"], "x1": [1, 2, 3]}),
    )


def test_sql_expr_trailing_semicolon_allowed():
    """A single trailing `;` terminator is still valid."""
    lk.models.DataFrameExpr(expr="SELECT id FROM {df};")
    lk.models.DataFrameExpr(expr="SELECT id FROM {df}; -- done")


def test_sql_expr_semicolon_in_literal_allowed():
    """A `;` inside a string or backtick-quoted identifier is not a separator."""
    lk.models.DataFrameExpr(expr="SELECT 'a;b' AS x FROM {df}")
    lk.models.DataFrameExpr(expr="SELECT `a;b` FROM {df}")


def test_sql_expr_rejects_multi_statement():
    """A genuine multi-statement `expr` raises a clear validation error."""
    with pytest.raises(ValidationError, match="single SQL statement"):
        lk.models.DataFrameExpr(expr="SELECT id FROM {df}; SELECT id FROM {df}")


def test_sql_expr_rejects_semicolon_mid_statement():
    """A `;` followed by more statement content (not just a trailing
    terminator) raises, even without a second full statement."""
    with pytest.raises(ValidationError, match="single SQL statement"):
        lk.models.DataFrameExpr(expr="SELECT id; FROM {df}")
