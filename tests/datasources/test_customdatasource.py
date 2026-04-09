import narwhals as nw
import polars as pl
import pytest

import laktory
from laktory import models
from laktory.models import LaktoryContext

from ..conftest import assert_dfs_equal

spark = laktory.get_spark_session()

# Reference data
_data = {"symbol": ["S0", "S1", "S2"], "close": [0.42, 0.50, 0.47]}
_df_target = pl.DataFrame(_data)


# --------------------------------------------------------------------------- #
# Custom read functions (module-level for importlib resolution)               #
# --------------------------------------------------------------------------- #


def _read_polars() -> pl.LazyFrame:
    return pl.DataFrame(_data).lazy()


def _read_spark():
    import pandas as pd

    return spark.createDataFrame(pd.DataFrame(_data))


def _read_with_kwargs(multiplier=1.0) -> pl.LazyFrame:
    return pl.DataFrame(
        {"symbol": _data["symbol"], "close": [v * multiplier for v in _data["close"]]}
    ).lazy()


# Context capture
_captured_context: LaktoryContext = None


def _read_capture_context(laktory_context: LaktoryContext = None) -> pl.LazyFrame:
    global _captured_context
    _captured_context = laktory_context
    return pl.DataFrame(_data).lazy()


# --------------------------------------------------------------------------- #
# Model Validation                                                            #
# --------------------------------------------------------------------------- #


def test_model():
    source = models.CustomDataSource(
        custom_reader={
            "func_name": "tests.datasources.test_customdatasource._read_polars"
        },
    )
    assert (
        source.custom_reader.func_name
        == "tests.datasources.test_customdatasource._read_polars"
    )
    assert source.type == "CUSTOM"


def test_string_coercion():
    source = models.CustomDataSource(
        custom_reader="tests.datasources.test_customdatasource._read_polars",
    )
    assert isinstance(source.custom_reader, models.CustomReader)
    assert (
        source.custom_reader.func_name
        == "tests.datasources.test_customdatasource._read_polars"
    )
    assert source.custom_reader.func_args == []
    assert source.custom_reader.func_kwargs == {}


def test_kwargs():
    source = models.CustomDataSource(
        custom_reader={
            "func_name": "tests.datasources.test_customdatasource._read_with_kwargs",
            "func_kwargs": {"multiplier": 2.0},
        },
    )
    assert source.custom_reader.func_kwargs == {"multiplier": 2.0}


# --------------------------------------------------------------------------- #
# Read                                                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "func_name",
    [
        "tests.datasources.test_customdatasource._read_polars",
        "tests.datasources.test_customdatasource._read_spark",
    ],
)
def test_read(func_name):
    source = models.CustomDataSource(custom_reader=func_name)
    df = source.read()
    assert isinstance(df, (nw.DataFrame, nw.LazyFrame))
    assert_dfs_equal(df, _df_target)


def test_read_with_kwargs():
    source = models.CustomDataSource(
        custom_reader={
            "func_name": "tests.datasources.test_customdatasource._read_with_kwargs",
            "func_kwargs": {"multiplier": 1.0},
        },
    )
    df = source.read()
    assert_dfs_equal(df, _df_target)


def test_post_read():
    """Inherited post-read operations (selects, filter, renames) still apply."""
    source = models.CustomDataSource(
        custom_reader="tests.datasources.test_customdatasource._read_polars",
        selects=["symbol"],
    )
    df = source.read()
    assert df.columns == ["symbol"]


# --------------------------------------------------------------------------- #
# LaktoryContext injection                                                    #
# --------------------------------------------------------------------------- #


def test_laktory_context():
    global _captured_context
    source = models.CustomDataSource(
        custom_reader="tests.datasources.test_customdatasource._read_capture_context",
    )
    source.read()

    assert isinstance(_captured_context, LaktoryContext)
    assert _captured_context.source is source
    assert _captured_context.node is None
    assert _captured_context.pipeline is None
    assert _captured_context.sink is None


def test_laktory_context_not_injected_when_not_declared():
    """Functions without laktory_context in their signature work unchanged."""
    source = models.CustomDataSource(
        custom_reader="tests.datasources.test_customdatasource._read_polars",
    )
    # Would raise TypeError if laktory_context were injected into _read_polars
    df = source.read()
    assert_dfs_equal(df, _df_target)
