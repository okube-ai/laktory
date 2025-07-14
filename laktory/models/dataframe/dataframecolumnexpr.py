import re
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import Union

import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild

if TYPE_CHECKING:
    import polars as pl
    import pyspark.sql.functions as F


logger = get_logger(__name__)

if TYPE_CHECKING:
    import polars as pl
    import pyspark.sql.functions as F


class DataFrameColumnExpr(BaseModel, PipelineChild):
    """
    DataFrame Column Expression defined with a string representation of DataFrame
    API expression (native or Narwhals) or a SQL statement.

    Examples
    --------
    Define serializable expressions in native DataFrame API.
    ```py
    import polars as pl

    import laktory as lk

    df = pl.DataFrame(
        {
            "x": [1, 2, 3],
        }
    )

    expr1 = lk.models.DataFrameColumnExpr(
        expr="pl.col('x')+pl.lit(1)",
        dataframe_backend="POLARS",
        dataframe_api="NATIVE",
    )

    expr2 = lk.models.DataFrameColumnExpr(
        expr="2*x + 1",
        type="SQL",
        dataframe_backend="POLARS",
        dataframe_api="NATIVE",
    )

    df = df.with_columns(
        y1=expr1.to_expr(),
        y2=expr2.to_expr(),
    )

    print(df)
    '''
    | x | y1 | y2 |
    |---|----|----|
    | 1 | 2  | 3  |
    | 2 | 3  | 5  |
    | 3 | 4  | 7  |
    '''
    ```

    Define serializable expressions in Narwhals DataFrame API.
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk

    df = nw.from_native(
        pl.DataFrame(
            {
                "x": [1, 2, 3],
            }
        )
    )

    expr1 = lk.models.DataFrameColumnExpr(
        expr="nw.col('x')+nw.lit(1)",
        dataframe_backend="POLARS",
        dataframe_api="NARWHALS",
    )

    expr2 = lk.models.DataFrameColumnExpr(
        expr="2*x + 1",
        type="SQL",
        dataframe_backend="POLARS",
        dataframe_api="NARWHALS",
    )

    df = df.with_columns(
        y1=expr1.to_expr(),
        y2=expr2.to_expr(),
    )

    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    | | x | y1 | y2 |  |
    | |---|----|----|  |
    | | 1 | 2  | 3  |  |
    | | 2 | 3  | 5  |  |
    | | 3 | 4  | 7  |  |
    └──────────────────┘
    '''
    ```
    """

    expr: str = Field(..., description="Expression string representation")
    type: Literal["SQL", "DF"] = Field(
        None,
        description="Expression type: DF or SQL. If `None` is specified, type is guessed from provided expression.",
    )

    @model_validator(mode="after")
    def guess_type(self) -> Any:
        if self.type:
            return self

        expr_clean = self.expr.strip().replace("\n", " ")

        type = "SQL"
        if re.findall(r"\w+\.\w+\(", expr_clean):
            type = "DF"

        for k in [
            "lit(",
            "col(",
            "F.",
            "nw.",
            "pl.",
        ]:
            if k in expr_clean:
                type = "DF"
                break

        self.type = type

        return self

    # ----------------------------------------------------------------------- #
    # Expressions                                                             #
    # ----------------------------------------------------------------------- #

    def to_sql_expr(self) -> str:
        """Column expression expressed as a SQL Statement"""
        # -> pure SQL

        if self.type == "SQL":
            return self.expr
        else:
            # TODO: Use SQLFrame?
            raise ValueError("DataFrame expression can't be converted to SQL")

    def to_expr(self) -> Union[nw.Expr, "pl.Expr", "F.Column"]:
        """Column expression expressed as DataFrame API object"""

        _value = self.expr.replace("\n", " ")

        if self.dataframe_api == "NARWHALS":
            if self.type == "SQL":
                from laktory.narwhals_ext.functions import sql_expr

                expr = sql_expr(_value)
            else:
                # Imports required to evaluate expressions
                import narwhals as nw  # noqa: F401
                from narwhals import col  # noqa: F401
                from narwhals import lit  # noqa: F401

                expr = eval(_value)
        else:
            if self.dataframe_backend == DataFrameBackends.PYSPARK:
                if self.type == "SQL":
                    import pyspark.sql.functions as F

                    expr = F.expr(_value)
                else:
                    # Imports required to evaluate expressions
                    import pyspark.sql.functions as F  # noqa: F401
                    import pyspark.sql.types as T  # noqa: F401
                    from pyspark.sql.functions import col  # noqa: F401
                    from pyspark.sql.functions import lit  # noqa: F401

                    expr = eval(_value)

            elif self.dataframe_backend == DataFrameBackends.POLARS:
                if self.type == "SQL":
                    import polars as pl

                    expr = pl.sql_expr(_value)
                else:
                    # Imports required to evaluate expressions
                    import polars as pl  # noqa: F401
                    import polars.functions as F  # noqa: F401
                    from polars import col  # noqa: F401
                    from polars import lit  # noqa: F401

                    expr = eval(self.expr)

            else:
                raise ValueError(
                    f"`dataframe_backend` '{self.dataframe_backend}' is not supported."
                )

        return expr
