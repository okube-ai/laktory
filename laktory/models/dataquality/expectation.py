from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.dataframecolumnexpression import DataFrameColumnExpression
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
        `WARN`: Write invalid records to the output DataFrame, but log
        exception.
        `DROP`: Drop Invalid records to the output DataFrame and log exception.
        `QUARANTINE`: Forward invalid data for quarantine.
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
        action="WARN",
        expr="close > 127",
        tolerance={"rel": 0.05},
    )
    print(dqe)
    '''
    variables={} action='WARN' type='ROW' name='price higher than 10' expr=DataFrameColumnExpression(variables={}, value='close > 127', type='SQL') tolerance=ExpectationTolerance(variables={}, abs=None, rel=0.05)
    '''

    dqe = models.DataQualityExpectation(
        name="rows count",
        expr="COUNT(*) > 50",
        type="AGGREGATE",
    )
    print(dqe)
    '''
    variables={} action='WARN' type='AGGREGATE' name='rows count' expr=DataFrameColumnExpression(variables={}, value='COUNT(*) > 50', type='SQL') tolerance=ExpectationTolerance(variables={}, abs=0, rel=None)
    '''
    ```

    References
    ----------

    * [DLT Table Expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)
    """

    action: Literal["WARN", "DROP", "QUARANTINE", "FAIL"] = "WARN"
    type: Literal["AGGREGATE", "ROW"] = "ROW"
    name: str
    expr: Union[str, DataFrameColumnExpression] = None
    tolerance: ExpectationTolerance = ExpectationTolerance(abs=0)
    _check: DataQualityCheck = None

    @model_validator(mode="after")
    def parse_expr(self) -> Any:
        if isinstance(self.expr, str):
            self.expr = DataFrameColumnExpression(value=self.expr)
        return self

    @model_validator(mode="after")
    def validate_action(self) -> Any:
        if self.type == "AGGREGATE" and self.action in ["DROP", "QUARANTINE"]:
            raise ValueError(f"'{self.type}' action is not supported for 'AGGREGATE' type.")
        return self

    @model_validator(mode="after")
    def warn_invalid_type(self):
        msg = self.type_warning_msg
        if msg:
            import warnings
            warnings.warn(msg)
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

    @property
    def type_warning_msg(self):
        msg = None
        if self.type != "AGGREGATE":
            for k in [
                "count(",
                "sum(",
                "avg(",
                "mean(",
                "max(",
                "min(",
                "variance(",
                "stddev(",
                "row_number(",
                "rank(",
            ]:
                if k in self.expr.value.lower():
                    msg = f"'ROW' type is selected for expectation '{self.name}' ({self.expr.value}). Should probably be 'AGGREGATE'."
                    break
        return msg

    # ----------------------------------------------------------------------- #
    # Execution                                                               #
    # ----------------------------------------------------------------------- #

    def check(self, df: Any) -> DataQualityCheck:

        logger.info(f"Checking expectation '{self.name}' | {self.expr.value} (type: {self.type})")

        rows_count = df.count()

        if self.type == "ROW":
            try:
                df_fail = df.filter(self.fail_filter)
            except Exception as e:
                if "Rewrite the query to avoid window functions" in getattr(e, "desc", ""):
                    e.desc += f"\n{self.type_warning_msg}"
                raise e

            fails_count = df_fail.count()

            status = "PASS"

            if self.tolerance.abs is not None:
                if fails_count > self.tolerance.abs:
                    status = "FAIL"
            elif self.tolerance.rel is not None:
                if rows_count > 0 and fails_count / rows_count > self.tolerance.rel:
                    status = "FAIL"

            self._check = DataQualityCheck(
                expectation=self,
                fails_count=fails_count,
                status=status,
                rows_count=rows_count,
            )
            failure_str = f"({100*self._check.failure_rate:5.2f}%)"
            if status == "PASS":
                logger.info(f"Checking expectation '{self.name}' | status : {status}")
            else:
                logger.info(f"Checking expectation '{self.name}' | status : {status} - failed rows : {fails_count} {failure_str}")

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
            if status == "PASS":
                logger.info(f"Checking expectation '{self.name}' | status : {status}")
            else:
                logger.error(f"Checking expectation '{self.name}' | status : {status}")

        return self._check
