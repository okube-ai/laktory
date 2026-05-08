"""
Verify that registering Laktory namespaces does not conflict with user-registered
namespaces, and that user registrations do not affect the laktory namespace.
"""

import narwhals as nw
import polars as pl

from laktory.narwhals_ext.namespace import NameSpace


def test_user_dataframe_namespace_coexists(monkeypatch):
    """User DataFrame/LazyFrame namespace coexists with laktory's without interference."""

    class _UserNs:
        def __init__(self, _df):
            self._df = _df

        def col_count(self):
            return len(self._df.columns)

    ns = NameSpace("_user_ns", _UserNs)
    monkeypatch.setattr(nw.DataFrame, "_user_ns", ns, raising=False)
    monkeypatch.setattr(nw.LazyFrame, "_user_ns", ns, raising=False)

    df = nw.from_native(pl.DataFrame({"a": [1, 2], "b": [3, 4]}))

    # Laktory namespace is unaffected by the new registration
    assert "a" in df.laktory.schema_flat()
    assert "b" in df.laktory.schema_flat()

    # User namespace works independently
    assert df._user_ns.col_count() == 2


def test_user_expr_namespace_coexists(monkeypatch):
    """User Expr namespace coexists with laktory's without interference."""

    class _UserExprNs:
        def __init__(self, _expr):
            self._expr = _expr

        def doubled(self):
            return self._expr * 2

    ns = NameSpace("_user_expr_ns", _UserExprNs)
    monkeypatch.setattr(nw.Expr, "_user_expr_ns", ns, raising=False)

    df = nw.from_native(pl.DataFrame({"x": [1, 2, 3]}))

    # User expr namespace works
    result = df.with_columns(x2=nw.col("x")._user_expr_ns.doubled())
    assert result["x2"].to_list() == [2, 4, 6]

    # Laktory expr namespace is unaffected
    result2 = df.with_columns(xr=nw.col("x").laktory.roundp(p=1.0))
    assert result2["xr"].to_list() == [1.0, 2.0, 3.0]


def test_laktory_namespace_not_overwritten_by_user(monkeypatch):
    """Registering a different-named namespace does not overwrite laktory's."""

    class _OtherNs:
        def __init__(self, _df):
            self._df = _df

    monkeypatch.setattr(
        nw.DataFrame, "_other_ns", NameSpace("_other_ns", _OtherNs), raising=False
    )
    monkeypatch.setattr(
        nw.LazyFrame, "_other_ns", NameSpace("_other_ns", _OtherNs), raising=False
    )

    df = nw.from_native(pl.DataFrame({"x": [1]}))

    # laktory attribute is still the Laktory namespace class, not _OtherNs
    assert hasattr(df, "laktory")
    assert df.laktory.__class__.__name__ != "_OtherNs"
    assert hasattr(df.laktory, "schema_flat")
