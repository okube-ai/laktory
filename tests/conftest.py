import narwhals as nw
import pytest

from laktory import get_spark_session

# --------------------------------------------------------------------------- #
# DataFrame                                                                   #
# --------------------------------------------------------------------------- #


def assert_dfs_equal(result, expected, sort=True) -> None:
    # Convert to Narwhals
    result = nw.from_native(result)
    expected = nw.from_native(expected)

    # Convert to pandas
    if isinstance(result, nw.LazyFrame):
        result = result.collect("pandas")
    result = result.to_polars()

    if isinstance(expected, nw.LazyFrame):
        expected = expected.collect("pandas")
    expected = expected.to_polars()

    # Compare columns
    assert sorted(result.columns) == sorted(expected.columns)
    columns = result.columns

    # Compare rows
    assert result.height == expected.height

    if sort:
        result = result.sort(columns)
        expected = expected.sort(columns)

    # Compare content
    for c in columns:
        r = result[c].to_list()
        e = expected[c].to_list()
        if r != e:
            print(c, r, e)
        assert r == e


# --------------------------------------------------------------------------- #
# Spark                                                                       #
# --------------------------------------------------------------------------- #


@pytest.fixture()
def spark_dbks():
    raise NotImplementedError()


@pytest.fixture()
def spark():
    return get_spark_session()
