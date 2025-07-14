import warnings
from typing import Any
from typing import Literal

import narwhals as nw
from narwhals import Expr
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.exceptions import DataQualityCheckFailedError
from laktory.models.basemodel import BaseModel
from laktory.models.dataframe.dataframecolumnexpr import DataFrameColumnExpr
from laktory.models.dataquality.check import DataQualityCheck
from laktory.models.pipelinechild import PipelineChild
from laktory.typing import AnyFrame

logger = get_logger(__name__)


class ExpectationTolerance(BaseModel):
    """
    Tolerance values for data quality expectations with support for either
    absolute or relative tolerances.
    """

    abs: int = Field(
        None, description="Maximum number of rows with failure for a PASS status"
    )
    rel: float = Field(
        None, description="Relative number of rows with failure for a PASS status"
    )

    @model_validator(mode="after")
    def at_least_one(self) -> Any:
        if self.abs is None and self.rel is None:
            raise ValueError("At least `abs` or `rel` must be set.")

        if not (self.abs is None or self.rel is None):
            raise ValueError("Only one of `abs` or `rel` must be set.")

        return self


class DataQualityExpectation(BaseModel, PipelineChild):
    """
    Data Quality Expectation for a given DataFrame expressed as a row-specific
    condition (`type="ROW"`) or as an aggregated metric (`type="AGGREGATE"`).

    The expression may be defined as a SQL statement or a DataFrame expression.

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
    dataframe_backend_=None dataframe_api_=None variables={} action='WARN' type='ROW' name='price higher than 10' expr=DataFrameColumnExpr(dataframe_backend_=None, dataframe_api_=None, variables={}, expr='close > 127', type='SQL', dataframe_backend=<DataFrameBackends.PYSPARK: 'PYSPARK'>, dataframe_api='NARWHALS') tolerance=ExpectationTolerance(variables={}, abs=None, rel=0.05) dataframe_backend=<DataFrameBackends.PYSPARK: 'PYSPARK'> dataframe_api='NARWHALS'
    '''

    dqe = models.DataQualityExpectation(
        name="rows count",
        expr="COUNT(*) > 50",
        type="AGGREGATE",
    )
    print(dqe)
    '''
    dataframe_backend_=None dataframe_api_=None variables={} action='WARN' type='AGGREGATE' name='rows count' expr=DataFrameColumnExpr(dataframe_backend_=None, dataframe_api_=None, variables={}, expr='COUNT(*) > 50', type='SQL', dataframe_backend=<DataFrameBackends.PYSPARK: 'PYSPARK'>, dataframe_api='NARWHALS') tolerance=ExpectationTolerance(variables={}, abs=0, rel=None) dataframe_backend=<DataFrameBackends.PYSPARK: 'PYSPARK'> dataframe_api='NARWHALS'
    '''
    ```

    References
    ----------
    * [Data Quality](https://www.laktory.ai/concepts/dataquality/)
    * [DLT Table Expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)
    """

    action: Literal["WARN", "DROP", "QUARANTINE", "FAIL"] = Field(
        "WARN",
        description="""
        Action to take when expectation is not met.
        - `WARN`: Write invalid records to the output DataFrame, but log
        exception.
        - `DROP`: Drop Invalid records to the output DataFrame and log exception.
        - `QUARANTINE`: Forward invalid data for quarantine.
        - `FAIL`: Raise exception when invalid records are found.
        """,
    )
    type: Literal["AGGREGATE", "ROW"] = Field(
        "ROW",
        description="""
        Type of expectation:
        - `"ROW"`: Row-specific condition. Must be a boolean expression.
        - `"AGGREGATE"`: Global condition. Must be a boolean expression.
        """,
    )
    name: str = Field(..., description="Name of the expectation")
    expr: str | DataFrameColumnExpr = Field(
        None,
        description=" SQL or DataFrame expression representing a row-specific condition or an aggregated metric.",
    )
    tolerance: ExpectationTolerance = Field(
        ExpectationTolerance(abs=0),
        description="Tolerance for non-matching rows before resulting in failure. Only available for 'ROW' type expectation.",
    )
    _check: DataQualityCheck = None

    @model_validator(mode="after")
    def parse_expr(self) -> Any:
        if isinstance(self.expr, str):
            self.expr = DataFrameColumnExpr(expr=self.expr)
        return self

    @model_validator(mode="after")
    def validate_action(self) -> Any:
        if self.type == "AGGREGATE" and self.action in ["DROP", "QUARANTINE"]:
            raise ValueError(
                f"'{self.type}' action is not supported for 'AGGREGATE' type."
            )
        return self

    @model_validator(mode="after")
    def warn_invalid_type(self):
        msg = self.type_warning_msg
        if msg:
            import warnings

            warnings.warn(msg)
        return self

    # ----------------------------------------------------------------------- #
    # Filters                                                                 #
    # ----------------------------------------------------------------------- #

    @property
    def pass_filter(self) -> Expr | None:
        """Expression representing all rows meeting the expectation."""
        return self.expr.to_expr()

    @property
    def fail_filter(self) -> Expr | None:
        """Expression representing all rows not meeting the expectation."""
        return ~self.expr.to_expr()

    @property
    def keep_filter(self) -> Expr | None:
        """
        Expression representing all rows to keep, considering both the
        expectation and the selected action.
        """
        # if self._check is None:
        #     raise ValueError()
        if self.type != "ROW":
            return None
        if self.action not in ["DROP", "QUARANTINE"]:
            return None
        # if self._check.fails_count == 0:
        #     return None
        return self.pass_filter

    @property
    def quarantine_filter(self) -> Expr | None:
        """
        Expression representing all rows to quarantine, considering both the
        expectation and the selected action.
        """
        # if self._check is None:
        #     raise ValueError()
        if self.type != "ROW":
            return None
        if self.action not in ["QUARANTINE"]:
            return None
        # if self._check.fails_count == 0:
        #     return None
        return self.fail_filter

    # ----------------------------------------------------------------------- #
    # Compatibility                                                           #
    # ----------------------------------------------------------------------- #

    @property
    def is_dlt_compatible(self) -> bool:
        """Expectation is supported by DLT"""
        return self.expr.type == "SQL" and self.type == "ROW"

    @property
    def is_dlt_managed(self) -> bool:
        """Expectation is DLT-compatible and pipeline node is executed by DLT"""

        if not self.is_dlt_compatible:
            return False

        pl = self.parent_pipeline
        if pl is None:
            return False

        if not pl.is_orchestrator_dlt:
            return False

        from laktory import is_dlt_execute

        return is_dlt_execute()

    @property
    def is_streaming_compatible(self):
        return (
            self.type == "ROW"
            and self.tolerance.rel is None
            and self.tolerance.abs == 0
        )

    # ----------------------------------------------------------------------- #
    # Logging                                                                 #
    # ----------------------------------------------------------------------- #

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
                if k in self.expr.expr.lower():
                    msg = f"'ROW' type is selected for expectation '{self.name}' ({self.expr.expr}). Should probably be 'AGGREGATE'."
                    break
        return msg

    @property
    def log_msg(self) -> str:
        msg = f"expr: {self.expr.expr} | status: {self.check.status}"
        if self.type == "ROW" and self.check.fails_count:
            msg += f" | fails count: {self.check.fails_count} / {self.check.rows_count} ({100 * self.check.failure_rate:5.2f} %)"
        return msg

    # ----------------------------------------------------------------------- #
    # Check                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def check(self):
        return self._check

    def run_check(
        self,
        df: AnyFrame,
        raise_or_warn: bool = False,
        node=None,
    ) -> DataQualityCheck:
        """
        Check if expectation is met save result.

        Parameters
        ----------
        df:
            Input DataFrame for checking the expectation.
        raise_or_warn:
            Raise exception or issue warning if expectation is not met.
        node:
            Pipeline Node

        Returns
        -------
        output: DataQualityCheck
            Check result.
        """

        logger.info(
            f"Checking expectation '{self.name}' | {self.expr.expr} (type: {self.type})"
        )

        # Run Check
        self._check = self._check_df(df)

        if raise_or_warn:
            self.raise_or_warn(node)

        return self._check

    def _check_df(self, df):
        if isinstance(df, nw.LazyFrame):
            if (
                DataFrameBackends.from_nw_implementation(df.implementation)
                == DataFrameBackends.PYSPARK
            ):
                # Using the pandas backend to avoid pyarrow version compatibility issues
                df = df.collect(backend="pandas")
            else:
                df = df.collect()
        rows_count = df.shape[0]

        if rows_count == 0:
            _check = DataQualityCheck(
                fails_count=0,
                status="PASS",
                rows_count=0,
            )
            return _check

        if self.type == "ROW":
            try:
                df_fail = df.filter(self.fail_filter)
            except Exception as e:
                if "Rewrite the query to avoid window functions" in getattr(
                    e, "desc", ""
                ):
                    e.desc += f"\n{self.type_warning_msg}"
                raise e

            fails_count = df_fail.shape[0]

            status = "PASS"
            if self.tolerance.abs is not None:
                if fails_count > self.tolerance.abs:
                    status = "FAIL"
            elif self.tolerance.rel is not None:
                if rows_count > 0 and fails_count / rows_count > self.tolerance.rel:
                    status = "FAIL"

            _check = DataQualityCheck(
                fails_count=fails_count,
                status=status,
                rows_count=rows_count,
            )
            failure_str = f"({100 * _check.failure_rate:5.2f}%)"
            if status == "PASS":
                logger.info(f"Checking expectation '{self.name}' | status : {status}")
            else:
                logger.info(
                    f"Checking expectation '{self.name}' | status : {status} - failed rows : {fails_count} {failure_str}"
                )
            return _check

        if self.type == "AGGREGATE":
            _df = df.select(self.expr.to_expr()).to_pandas()
            #
            # import pyspark.sql.functions as F  # noqa: F401
            #
            # if self.expr.type == "SQL":
            #     _df = df.select(self.expr.eval()).toPandas()
            # else:
            #     _df = df.agg(self.expr.eval()).toPandas()

            status = _df.iloc[0].values[0]

            status = "PASS" if status else "FAIL"

            _check = DataQualityCheck(
                status=status,
                rows_count=rows_count,
            )
            logger.info(f"Checking expectation '{self.name}' | status : {status}")
            return _check

    def raise_or_warn(self, node=None) -> None:
        """
        Raise exception or issue warning if expectation is not met.
        """

        # Failure Message
        msg = f"Expectation '{self.name}'"
        if node:
            msg += f" for node '{node.name}'"
        msg += f" FAILED | {self.log_msg}"

        if self.check.status != "FAIL":
            return

        # Raise Exception
        if self.action == "FAIL":
            raise DataQualityCheckFailedError(self, node)
        else:
            # actions: WARN, DROP, QUARANTINE
            warnings.warn(msg)
