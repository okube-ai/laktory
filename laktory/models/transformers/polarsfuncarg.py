from pydantic import field_validator
from typing import Any
from typing import Union

from laktory.models.basemodel import BaseModel


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class PolarsFuncArg(BaseModel):
    """
    Polars function argument

    Attributes
    ----------
    value:
        Value of the argument
    """

    value: Union[Any]

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

        # Imports required to evaluate expressions
        import polars.functions as F
        from polars import col
        from polars import lit
        from polars import sql_expr

        v = self.value

        if isinstance(v, BaseDataSource):
            v = self.value.read()
        elif isinstance(v, str):
            for f in ["lit", "col", "sql_expr", "F."]:
                if f in v:
                    v = eval(v)
                    break

        return v
