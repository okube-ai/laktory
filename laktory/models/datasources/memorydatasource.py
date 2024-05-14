from typing import Any
from pydantic import model_validator

from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.types import AnyDataFrame
from laktory.spark import SparkDataFrame
from laktory.spark import is_spark_dataframe
from laktory.polars import PolarsDataFrame
from laktory.polars import is_polars_dataframe
from laktory._logger import get_logger

logger = get_logger(__name__)


class MemoryDataSource(BaseDataSource):
    """
    Data source using in-memory DataFrame, generally used in the context of a
    data pipeline.

    Attributes
    ----------
    df
        Input DataFrame

    Examples
    ---------
    ```python
    from laktory import models
    import pandas as pd

    df = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL"],
                "price": [200.0, 205.0],
                "tstamp": ["2023-09-01", "2023-09-01"],
            }
        )
    )

    source = models.MemoryDataSource(
        df=df,
        as_stream=False,
    )
    df = source.read()
    ```
    """

    df: Any

    @model_validator(mode="after")
    def set_dataframe_type(self) -> Any:

        if is_spark_dataframe(self.df):
            dataframe_type = "SPARK"
        elif is_polars_dataframe(self.df):
            dataframe_type = "POLARS"
        else:
            raise ValueError("DataFrame must be of type Spark or Polars")

        self.dataframe_type = dataframe_type

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #
    #
    # @property
    # def _id(self):
    #     return str(self.path)

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_spark(self, spark) -> SparkDataFrame:
        logger.info(f"Reading {self._id} from memory")
        return self.df

    def _read_polars(self) -> PolarsDataFrame:
        logger.info(f"Reading {self._id} from memory")
        return self.df
