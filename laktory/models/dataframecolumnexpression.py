import re
from typing import Any
from typing import Literal

from pydantic import model_validator

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.typing import AnyDataFrameColumn

logger = get_logger(__name__)


class DataFrameColumnExpression(BaseModel):
    """
    DataFrame Column Expression supporting SQL statements or string
    representation of DataFrame API expression.

    Attributes
    ----------
    value:
        String representation
    type:
        Expression type: DF or SQL. If `None` is specified, type is guessed.

    Examples
    --------
    ```py
    from laktory import models

    e1 = models.DataFrameColumnExpression(
        value="MAX(close)",
    )
    print(e1.eval())
    # > Column<'MAX(close)'>

    e2 = models.DataFrameColumnExpression(
        value="F.abs('close')",
    )
    print(e2.eval())
    # > Column<'abs(close)'>
    ```
    """

    value: str
    type: Literal["SQL", "DF"] = None

    @model_validator(mode="after")
    def guess_type(self) -> Any:
        if self.type:
            return self

        expr_clean = self.value.strip().replace("\n", " ")

        type = "SQL"
        if re.findall(r"\w+\.\w+\(", expr_clean):
            type = "DF"
        if "lit(" in expr_clean:
            type = "DF"
        if "col(" in expr_clean:
            type = "DF"

        self.type = type

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def df_expr(self, dataframe_backend="SPARK"):
        expr = self.value
        if self.type == "SQL":
            _value = repr(self.value.replace("\n", " "))
            expr = f"F.expr({_value})"
            if dataframe_backend == "POLARS":
                expr = f"pl.Expr.laktory.sql_expr({_value})"

        return expr

    def eval(self, udfs=None, dataframe_backend=None) -> AnyDataFrameColumn:
        if dataframe_backend is None:
            dataframe_backend = settings.dataframe_backend

        # Adding udfs to global variables
        if udfs is None:
            udfs = {}
        for k, v in udfs.items():
            globals()[k] = v

        if dataframe_backend == "SPARK":
            # Imports required to evaluate expressions
            import pyspark.sql.functions as F  # noqa: F401
            import pyspark.sql.types as T  # noqa: F401
            from pyspark.sql.functions import col  # noqa: F401
            from pyspark.sql.functions import lit  # noqa: F401

        elif dataframe_backend == "POLARS":
            # Imports required to evaluate expressions
            import polars as pl  # noqa: F401
            import polars.functions as F  # noqa: F401
            from polars import col  # noqa: F401
            from polars import lit  # noqa: F401

        else:
            raise ValueError(
                f"`dataframe_backend` '{dataframe_backend}' is not supported."
            )

        _expr_str = self.df_expr(dataframe_backend=dataframe_backend)
        expr = eval(_expr_str)

        # Cleaning up global variables
        for k, v in udfs.items():
            del globals()[k]

        return expr
