from pydantic import field_validator
from typing import Any
from typing import Literal
from typing import Union

from laktory.models.basemodel import BaseModel


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class ChainNodeFuncArg(BaseModel):
    """
    Base function argument

    Attributes
    ----------
    value:
        Value of the argument
    """

    value: Union[Any]
    dataframe_type: Literal["SPARK", "POLARS"] = "SPARK"

    @field_validator("value")
    def value_to_data_source(cls, v: Any) -> Any:
        """
        Data source can't be set as an expected value type as it would create
        a circular dependency. Instead, we check at validation if the value
        could be instantiated as a DataSource object.
        """
        from laktory.models.datasources import classes

        for c in classes:
            try:
                v = c(**v)
                break
            except:
                pass

        return v

    def eval(self):
        from laktory.models.datasources.basedatasource import BaseDataSource

        v = self.value

        if isinstance(v, BaseDataSource):
            v = self.value.read()
        elif isinstance(v, str):

            if self.dataframe_type == "SPARK":

                # Imports required to evaluate expressions
                import pyspark.sql.functions as F
                from pyspark.sql.functions import lit
                from pyspark.sql.functions import col
                from pyspark.sql.functions import expr

                targets = ["lit(", "col(", "expr(", "F."]

            elif self.dataframe_type == "POLARS":

                # Imports required to evaluate expressions
                import polars as pl
                from polars import col
                from polars import lit
                from polars import sql_expr

                targets = ["lit(", "col(", "sql_expr(", "pl."]

            else:
                raise ValueError(f"DataFrame type '{self.dataframe_type}' is not supported")

            for f in targets:
                if f in v:
                    v = eval(v)
                    break

        return v

    def signature(self):
        if self.dataframe_type == "SPARK":
            return str(self.value)
        elif self.dataframe_type == "POLARS":
            import polars as pl
            eval = self.eval()
            if isinstance(eval, pl.DataFrame):
                return f"{eval.laktory.signature()}"
            else:
                return f"{self.value}"
        else:
            raise ValueError(f"DataFrame type '{self.dataframe_type}' is not supported")

