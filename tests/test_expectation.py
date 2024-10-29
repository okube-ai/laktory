import pytest
from pyspark.sql import functions as F

from laktory import models
from laktory.exceptions import DataQualityCheckFailedError
from laktory._testing import Paths
from laktory._testing import df_slv as df

paths = Paths(__file__)


def test_expectations_abs():

    # Spark Expression - WARN
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="WARN", expr="F.col('close') < 300"
    )
    check = dqe.run_check(df)
    assert check.rows_count == 80
    assert check.fails_count == 20
    assert check.failure_rate == 0.25
    assert check.status == "FAIL"
    assert str(dqe.pass_filter) == str(F.col("close") < 300)
    assert str(dqe.fail_filter) == str(~(F.col("close") < 300))
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None

    # Spark Expression - QUARANTINE
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="DROP", expr="F.col('close') < 300"
    )
    check = dqe.run_check(df)
    assert check.rows_count == 80
    assert check.fails_count == 20
    assert check.failure_rate == 0.25
    assert check.status == "FAIL"
    assert str(dqe.pass_filter) == str(F.col("close") < 300)
    assert str(dqe.fail_filter) == str(~(F.col("close") < 300))
    assert str(dqe.keep_filter) == str(F.col("close") < 300)
    assert dqe.quarantine_filter is None

    # Spark Expression - QUARANTINE
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="QUARANTINE", expr="F.col('close') < 300"
    )
    check = dqe.run_check(df)
    assert check.rows_count == 80
    assert check.fails_count == 20
    assert check.failure_rate == 0.25
    assert check.status == "FAIL"
    assert str(dqe.pass_filter) == str(F.col("close") < 300)
    assert str(dqe.fail_filter) == str(~(F.col("close") < 300))
    assert str(dqe.keep_filter) == str(F.col("close") < 300)
    assert str(dqe.quarantine_filter) == str(~(F.col("close") < 300))

    # SQL Expression
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="WARN", expr="close < 300"
    )
    check = dqe.run_check(df)
    assert check.rows_count == 80
    assert check.fails_count == 20
    assert check.failure_rate == 0.25
    assert check.status == "FAIL"
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None


def test_expectations_rel():

    dqe = models.DataQualityExpectation(
        name="price higher than 10",
        action="WARN",
        expr="close > 127",
        tolerance={"rel": 0.05},
    )
    check = dqe.run_check(df)
    assert check.rows_count == 80
    assert check.fails_count == 3
    assert check.failure_rate == 0.0375
    assert check.status == "PASS"
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None


def test_expectations_agg():

    dqe = models.DataQualityExpectation(
        name="rows count",
        expr="COUNT(*) > 50",
        type="AGGREGATE",
    )
    check = dqe.run_check(df)
    assert check.rows_count == 80
    assert check.fails_count is None
    assert check.failure_rate is None
    assert check.status == "PASS"
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None

    dqe = models.DataQualityExpectation(
        name="rows count",
        expr="F.count('*') > 90",
        type="AGGREGATE",
    )
    check = dqe.run_check(df)
    assert check.rows_count == 80
    assert check.fails_count is None
    assert check.failure_rate is None
    assert check.status == "FAIL"
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None


def test_expectations_empty():

    # Spark Expression
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="WARN", expr="F.col('close') < 300"
    )
    check = dqe.run_check(df.filter("close < 0"))
    assert check.rows_count == 0
    assert check.fails_count == 0
    assert check.failure_rate == 0
    assert check.status == "PASS"
    assert dqe.keep_filter is None
    assert dqe.quarantine_filter is None


def test_expectations_exceptions_warnings():

    # No Failure
    dqe = models.DataQualityExpectation(
        name="price less than 900", action="FAIL", expr="F.col('close') < 900"
    )
    dqe.run_check(df, raise_or_warn=True)

    # Do Not Raise
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="WARN", expr="F.col('close') < 300"
    )
    dqe.run_check(df, raise_or_warn=False)

    # Warn
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="WARN", expr="F.col('close') < 300"
    )
    with pytest.warns(UserWarning):
        dqe.run_check(df, raise_or_warn=True)

    # Raise Exception
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="FAIL", expr="F.col('close') < 300"
    )
    with pytest.raises(DataQualityCheckFailedError):
        dqe.run_check(df, raise_or_warn=True)


if __name__ == "__main__":
    test_expectations_abs()
    test_expectations_rel()
    test_expectations_agg()
    test_expectations_empty()
    test_expectations_exceptions_warnings()
