import os
import shutil

from pyspark.sql import functions as F

from laktory import models
from laktory._testing import spark
from laktory._testing import Paths

paths = Paths(__file__)

# Data
df = spark.read.parquet(os.path.join(paths.data, "./slv_stock_prices"))
df.printSchema()


def test_expectations_abs():

    # Spark Expression
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="ALLOW", expr="F.col('close') < 300"
    )

    check = dqe.check(df)

    assert check.rows_count == 80
    assert check.fails_count == 20
    assert check.failure_rate == 0.25
    assert check.status == "FAIL"

    # SQL Expression
    dqe = models.DataQualityExpectation(
        name="price less than 300", action="ALLOW", expr="close < 300"
    )

    check = dqe.check(df)
    assert check.rows_count == 80
    assert check.fails_count == 20
    assert check.failure_rate == 0.25
    assert check.status == "FAIL"


def test_expectations_rel():

    dqe = models.DataQualityExpectation(
        name="price higher than 10",
        action="ALLOW",
        expr="close > 127",
        tolerance={"rel": 0.05},
    )

    check = dqe.check(df)
    assert check.rows_count == 80
    assert check.fails_count == 3
    assert check.failure_rate == 0.0375
    assert check.status == "PASS"


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


if __name__ == "__main__":
    test_expectations_abs()
    test_expectations_rel()
    test_expectations_agg()
