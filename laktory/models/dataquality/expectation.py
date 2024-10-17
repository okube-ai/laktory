from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.dataframeexpression import DataFrameExpression
from laktory.models.dataquality.check import DataQualityCheck
from laktory._logger import get_logger

logger = get_logger(__name__)


# class ExpectationThresholds(BaseModel):
#     min: float = None
#     max: float = None
#     strict_min: float = None
#     strict_max: float = None
#     in_: list[Any] = None
#     not_in: list[Any] = None
#     has: Any = None
#     has_not: Any = None
#     is_: Any = None
#     is_not: Any = None


class ExpectationTolerance(BaseModel):
    """
    Tolerance values for data quality expectations with support for either
    absolute or relative tolerances.

    Attributes
    ----------
    abs:
        Maximum number of rows with failure for a PASS status
    rel:
        Relative number of rows with failure for a PASS status
    """
    abs: int = None
    rel: float = None

    @model_validator(mode="after")
    def at_least_one(self) -> Any:
        if self.abs is None and self.rel is None:
            raise ValueError("At least `abs` or `rel` must be set.")

        if not (self.abs is None or self.rel is None):
            raise ValueError("Only one of `abs` or `rel` must be set.")

        return self


class DataQualityExpectation(BaseModel):
    """
    Data Quality Expectation for a given DataFrame expressed as a row-specific
    condition (`type="ROW"`) or as an aggregated metric (`type="AGGREGATE"`).

    The expression may be defined as a SQL statement or a DataFrame expression.

    Upon failure, an action may be selected.

    Attributes
    ----------
    action:
        Action to take when expectation is not met.
        `ALLOW`: Write invalid records to the output DataFrame, but log
        exception.
        `DROP`: Drop Invalid records to the output DataFrame and log exception.
        `FAIL`: Raise exception when invalid records are found.
    type:
        Type of expectation:
        `"ROW"`: Row-specific condition. Must be a boolean expression.
        `"AGGREGATE"`: Global condition. Must be a boolean expression.
    name:
        Name of the expectation
    expr:
        SQL or DataFrame expression representing a row-specific condition or
        an aggregated metric.
    tolerance:
        Tolerance for non-matching rows before resulting in failure. Only
        available for "ROW" type expectation.

    Examples
    --------
    ```py
    from laktory import models

    dqe = models.DataQualityExpectation(
        name="price higher than 10",
        action="ALLOW",
        expr="close > 127",
        tolerance={"rel": 0.05},
    )
    print(dqe)

    dqe = models.DataQualityExpectation(
        name="rows count",
        expr="COUNT(*) > 50",
        type="AGGREGATE",
    )
    print(dqe)
    ```

    References
    ----------

    * [DLT Table Expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)
    """

    action: Literal["ALLOW", "DROP", "FAIL"] = "ALLOW"
    type: Literal["AGGREGATE", "ROW"] = "ROW"
    name: str
    expr: Union[str, DataFrameExpression] = None
    tolerance: ExpectationTolerance = ExpectationTolerance(abs=0)
    _check: DataQualityCheck = None

    @model_validator(mode="after")
    def parse_expr(self) -> Any:
        if isinstance(self.expr, str):
            self.expr = DataFrameExpression(value=self.expr)
        return self

    @model_validator(mode="after")
    def validate_action(self) -> Any:
        if self.type == "AGGREGATE" and self.action == "DROP":
            raise ValueError("'DROP' action is not supported for 'AGGREGATE' type.")
        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def pass_filter(self):
        return self.expr.eval()

    @property
    def fail_filter(self):
        return ~self.expr.eval()

    # ----------------------------------------------------------------------- #
    # Execution                                                               #
    # ----------------------------------------------------------------------- #

    def check(self, df: Any) -> DataQualityCheck:

        logger.info(f"Checking expectation '{self.name}' | {self.expr.value} ({self.type})")

        rows_count = df.count()

        if self.type == "ROW":
            df_fail = df.filter(self.fail_filter)

            fails_count = df_fail.count()

            status = "PASS"

            if self.tolerance.abs is not None:
                if fails_count > self.tolerance.abs:
                    status = "FAIL"
            elif self.tolerance.rel is not None:
                if fails_count / rows_count > self.tolerance.rel:
                    status = "FAIL"

            self._check = DataQualityCheck(
                expectation=self,
                fails_count=fails_count,
                status=status,
                rows_count=rows_count,
            )
            logger.info(f"Checking expectation '{self.name}' | failed rows : {fails_count} ({self._check.failure_rate:5.2f}%)")
            logger.info(f"Checking expectation '{self.name}' | status : {status}")

        if self.type == "AGGREGATE":
            import pyspark.sql.functions as F

            if self.expr.type == "SQL":
                _df = df.select(self.expr.eval()).toPandas()
            else:
                _df = df.agg(self.expr.eval()).toPandas()

            status = _df.iloc[0].values[0]

            status = "PASS" if status else "FAIL"

            self._check = DataQualityCheck(
                expectation=self,
                status=status,
                rows_count=rows_count,
            )
            logger.info(f"Checking expectation '{self.name}' | status : {status}")

        return self._check
