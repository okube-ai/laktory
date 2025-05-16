import narwhals as nw
import pytest

from laktory import models
from laktory._testing import get_df0
from laktory.exceptions import DataQualityCheckFailedError


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_expectations_abs(backend):
    df0 = get_df0(backend, lazy=True)
    e = nw.col("x1") < 3
    e_str = "nw.col('x1') < 3"

    # Spark Expression - WARN
    dqe = models.DataQualityExpectation(
        name="x1 less than 3", action="WARN", expr=e_str
    )
    check = dqe.run_check(df0)
    assert check.rows_count == 3
    assert check.fails_count == 1
    assert check.failure_rate == 1 / 3.0
    assert check.status == "FAIL"
    assert str(dqe.pass_filter) == str(e)
    assert str(dqe.fail_filter) == str(~e)
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None

    # Spark Expression - QUARANTINE
    dqe = models.DataQualityExpectation(
        name="x1 less than 3", action="DROP", expr=e_str
    )
    check = dqe.run_check(df0)
    assert check.rows_count == 3
    assert check.fails_count == 1
    assert check.failure_rate == 1 / 3.0
    assert check.status == "FAIL"
    assert str(dqe.pass_filter) == str(e)
    assert str(dqe.fail_filter) == str(~e)
    assert str(dqe.keep_filter) == str(e)
    assert dqe.quarantine_filter is None

    # Spark Expression - QUARANTINE
    dqe = models.DataQualityExpectation(
        name="x1 less than 3", action="QUARANTINE", expr=e_str
    )
    check = dqe.run_check(df0)
    assert check.rows_count == 3
    assert check.fails_count == 1
    assert check.failure_rate == 1.0 / 3.0
    assert check.status == "FAIL"
    assert str(dqe.pass_filter) == str(e)
    assert str(dqe.fail_filter) == str(~e)
    assert str(dqe.keep_filter) == str(e)
    assert str(dqe.quarantine_filter) == str(~e)

    # SQL Expression
    dqe = models.DataQualityExpectation(
        name="x1 less than 3", action="WARN", expr="x1 < 3"
    )
    check = dqe.run_check(df0)
    assert check.rows_count == 3
    assert check.fails_count == 1
    assert check.failure_rate == 1.0 / 3.0
    assert check.status == "FAIL"
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_expectations_rel(backend):
    df0 = get_df0(backend, lazy=True)

    dqe = models.DataQualityExpectation(
        name="x1 less than 3",
        action="WARN",
        expr="x1 < 3",
        tolerance={"rel": 0.5},
    )
    check = dqe.run_check(df0)
    assert check.rows_count == 3
    assert check.fails_count == 1
    assert check.failure_rate == 1.0 / 3.0
    assert check.status == "PASS"
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_expectations_agg(backend):
    df0 = get_df0(backend, lazy=True)

    dqe = models.DataQualityExpectation(
        name="rows count",
        expr="COUNT(x1) > 2",
        type="AGGREGATE",
    )
    check = dqe.run_check(df0)
    assert check.rows_count == 3
    assert check.fails_count is None
    assert check.failure_rate is None
    assert check.status == "PASS"
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None

    dqe = models.DataQualityExpectation(
        name="rows count",
        expr="COUNT(x1) > 5",
        type="AGGREGATE",
    )
    check = dqe.run_check(df0)
    assert check.rows_count == 3
    assert check.fails_count is None
    assert check.failure_rate is None
    assert check.status == "FAIL"
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_expectations_empty(backend):
    df0 = get_df0(backend, lazy=True)

    # Spark Expression
    dqe = models.DataQualityExpectation(
        name="x1 less than 0", action="WARN", expr="nw.col('x1') < 0"
    )
    check = dqe.run_check(df0.filter("x1 < 0"))
    assert check.rows_count == 0
    assert check.fails_count == 0
    assert check.failure_rate == 0
    assert check.status == "PASS"
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_expectations_exceptions_warnings(backend):
    df0 = get_df0(backend)
    e_str = "nw.col('x1') < 3"

    # No Failure
    dqe = models.DataQualityExpectation(
        name="price less than 900", action="FAIL", expr="nw.col('x1') < 9"
    )
    dqe.run_check(df0, raise_or_warn=True)

    # Do Not Raise
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="WARN", expr=e_str
    )
    dqe.run_check(df0, raise_or_warn=False)

    # Warn
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="WARN", expr=e_str
    )
    with pytest.warns(UserWarning):
        dqe.run_check(df0, raise_or_warn=True)

    # Raise Exception
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="FAIL", expr=e_str
    )
    with pytest.raises(DataQualityCheckFailedError):
        dqe.run_check(df0, raise_or_warn=True)
