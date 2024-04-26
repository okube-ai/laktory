import re
from pydantic import model_validator
from typing import Any
from typing import Literal
from typing import Union

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.models.datasources.tabledatasource import TableDataSource


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
    value: Union[TableDataSource, Any]

    # @property
    # def is_column(self):
    #     return self.convert_to == "COLUMN"

    def eval(self, spark=None):

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
