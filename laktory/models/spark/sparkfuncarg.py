from pydantic import field_validator
from typing import Any
from typing import Union

from laktory.models.basemodel import BaseModel


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class SparkFuncArg(BaseModel):
    """
    Spark function argument

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
        from laktory.models.datasources.filedatasource import FileDataSource
        from laktory.models.datasources.tabledatasource import TableDataSource

        try:
            v = TableDataSource(**v)
        except:
            try:
                v = FileDataSource(**v)
            except:
                pass

        return v

    def eval(self, spark=None):
        from laktory.models.datasources.basedatasource import BaseDataSource

        # Imports required to evaluate expressions
        import pyspark.sql.functions as F
        from pyspark.sql.functions import lit
        from pyspark.sql.functions import col
        from pyspark.sql.functions import expr

        v = self.value

        if isinstance(v, BaseDataSource):
            v = self.value.read(spark)
        elif isinstance(v, str):
            for f in ["lit", "col", "expr", "F."]:
                if f in v:
                    v = eval(v)
                    break

        return v
