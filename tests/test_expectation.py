import os
import shutil

from pyspark.sql import functions as F

from laktory import models
from laktory._testing import Paths
from laktory._testing import df_slv as df
from laktory._testing import df_slv_delta as dfs

paths = Paths(__file__)


def test_expectations_abs():

    # Spark Expression - WARN
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="WARN", expr="F.col('close') < 300"
    )
    check = dqe.check(df)
    assert check.rows_count == 80
    assert check.fails_count == 20
    assert check.failure_rate == 0.25
    assert check.status == "FAIL"
    assert str(check.expectation.pass_filter) == str(F.col("close") < 300)
    assert str(check.expectation.fail_filter) == str(~(F.col("close") < 300))
    assert check.keep_filter is None
    assert check.quarantine_filter is None

    # Spark Expression - QUARANTINE
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="DROP", expr="F.col('close') < 300"
    )
    check = dqe.check(df)
    assert check.rows_count == 80
    assert check.fails_count == 20
    assert check.failure_rate == 0.25
    assert check.status == "FAIL"
    assert str(check.expectation.pass_filter) == str(F.col("close") < 300)
    assert str(check.expectation.fail_filter) == str(~(F.col("close") < 300))
    assert str(check.keep_filter) == str(F.col("close") < 300)
    assert check.quarantine_filter is None

    # Spark Expression - QUARANTINE
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="QUARANTINE", expr="F.col('close') < 300"
    )
    check = dqe.check(df)
    assert check.rows_count == 80
    assert check.fails_count == 20
    assert check.failure_rate == 0.25
    assert check.status == "FAIL"
    assert str(check.expectation.pass_filter) == str(F.col("close") < 300)
    assert str(check.expectation.fail_filter) == str(~(F.col("close") < 300))
    assert str(check.keep_filter) == str(F.col("close") < 300)
    assert str(check.quarantine_filter) == str(~(F.col("close") < 300))

    # SQL Expression
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="WARN", expr="close < 300"
    )
    check = dqe.check(df)
    assert check.rows_count == 80
    assert check.fails_count == 20
    assert check.failure_rate == 0.25
    assert check.status == "FAIL"
    assert check.keep_filter is None
    assert check.quarantine_filter is None


def test_expectations_rel():

    dqe = models.DataQualityExpectation(
        name="price higher than 10",
        action="WARN",
        expr="close > 127",
        tolerance={"rel": 0.05},
    )
    check = dqe.check(df)
    assert check.rows_count == 80
    assert check.fails_count == 3
    assert check.failure_rate == 0.0375
    assert check.status == "PASS"
    assert check.keep_filter is None
    assert check.quarantine_filter is None

def test_expectations_agg():

    dqe = models.DataQualityExpectation(
        name="rows count",
        expr="COUNT(*) > 50",
        type="AGGREGATE",
    )
    check = dqe.check(df)
    assert check.rows_count == 80
    assert check.fails_count is None
    assert check.failure_rate is None
    assert check.status == "PASS"
    assert check.keep_filter is None
    assert check.quarantine_filter is None

    dqe = models.DataQualityExpectation(
        name="rows count",
        expr="F.count('*') > 90",
        type="AGGREGATE",
    )
    check = dqe.check(df)
    assert check.rows_count == 80
    assert check.fails_count is None
    assert check.failure_rate is None
    assert check.status == "FAIL"
    assert check.keep_filter is None
    assert check.quarantine_filter is None


def test_expectations_empty():

    # Spark Expression
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="WARN", expr="F.col('close') < 300"
    )
    check = dqe.check(df.filter("close < 0"))
    assert check.rows_count == 0
    assert check.fails_count == 0
    assert check.failure_rate == 0
    assert check.status == "PASS"
    assert check.keep_filter is None
    assert check.quarantine_filter is None


def test_expectations_streaming():

    # Spark Expression
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="DROP", expr="F.col('close') < 300"
    )
    check = dqe.check(dfs)
    assert check.is_streaming
    assert check.rows_count is None
    assert check.fails_count is None
    assert check.failure_rate is None
    assert check.status == "UNDEFINED"
    assert str(check.keep_filter) == str(F.col("close") < 300)
    assert check.quarantine_filter is None


if __name__ == "__main__":
    test_expectations_abs()
    test_expectations_rel()
    test_expectations_agg()
    test_expectations_empty()
    test_expectations_streaming()
