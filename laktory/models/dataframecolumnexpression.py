import re
from typing import Literal
from typing import Any
from pydantic import model_validator

from laktory.types import AnyDataFrameColumn
from laktory.models.basemodel import BaseModel
from laktory._logger import get_logger

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
    #> Column<'MAX(close)'>

    e2 = models.DataFrameColumnExpression(
        value="F.abs('close')",
    )
    print(e2.eval())
    #> Column<'abs(close)'>
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

    def df_expr(self, dataframe_type="SPARK"):
        expr = self.value
        if self.type == "SQL":
            _value = repr(self.value.replace("\n", " "))
            expr = f"F.expr({_value})"
            if dataframe_type == "POLARS":
                expr = f"pl.Expr.laktory.sql_expr({_value})"

        return expr

    def eval(self, udfs=None, dataframe_type="SPARK") -> AnyDataFrameColumn:

        # Adding udfs to global variables
        if udfs is None:
            udfs = {}
        for k, v in udfs.items():
            globals()[k] = v

        if dataframe_type == "SPARK":
            # Imports required to evaluate expressions
            import pyspark.sql.functions as F
            import pyspark.sql.types as T
            from pyspark.sql.functions import col
            from pyspark.sql.functions import lit

        elif dataframe_type == "POLARS":
            # Imports required to evaluate expressions
            import polars as pl
            import polars.functions as F
            from polars import col
            from polars import lit

        else:
            raise ValueError(f"`dataframe_type` '{dataframe_type}' is not supported.")

        _expr_str = self.df_expr(dataframe_type=dataframe_type)
        expr = eval(_expr_str)

        # Cleaning up global variables
        for k, v in udfs.items():
            del globals()[k]

        return expr
