from typing import Any
from typing import Union

from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.polars import PolarsLazyFrame
from laktory.polars import is_polars_dataframe
from laktory.spark import SparkDataFrame
from laktory.spark import is_spark_dataframe

logger = get_logger(__name__)


class MemoryDataSource(BaseDataSource):
    """
    Data source using in-memory DataFrame, generally used in the context of a
    data pipeline.

    Attributes
    ----------
    data:
        Serialized data to build input DataFrame
    df:
        Input DataFrame

    Examples
    ---------
    ```python
    import polars as pl

    from laktory import models

    data = {
        "symbol": ["AAPL", "GOOGL"],
        "price": [200.0, 205.0],
        "tstamp": ["2023-09-01", "2023-09-01"],
    }

    # Spark from dict
    source = models.MemoryDataSource(
        data=data,
        dataframe_backend="SPARK",
    )
    df = source.read(spark=spark)
    print(df.laktory.show_string())
    '''
    +-----+------+----------+
    |price|symbol|    tstamp|
    +-----+------+----------+
    |200.0|  AAPL|2023-09-01|
    |205.0| GOOGL|2023-09-01|
    +-----+------+----------+
    '''

    # Polars from df
    source = models.MemoryDataSource(
        df=pl.DataFrame(data),
    )
    df = source.read()
    print(df.to_pandas())
    '''
      symbol  price      tstamp
    0   AAPL  200.0  2023-09-01
    1  GOOGL  205.0  2023-09-01
    '''
    ```
    """

    data: Union[dict[str, list[Any]], list[dict[str, Any]]] = None
    df: Any = None

    @model_validator(mode="after")
    def validate_input(self) -> Any:
        if self.df is None and self.data is None:
            raise ValueError("Either `data` or `df` must be provided.")

        if self.df is not None and self.data is not None:
            raise ValueError("Only `data` or `df` can be provided.")

        if self.df is not None:
            if is_spark_dataframe(self.df):
                dataframe_backend = "SPARK"
            elif is_polars_dataframe(self.df):
                dataframe_backend = "POLARS"
            else:
                raise ValueError("DataFrame must be of type Spark or Polars")

            with self.validate_assignment_disabled():
                self.dataframe_backend = dataframe_backend

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

        if self.df is not None:
            return self.df

        data = self.data
        if isinstance(data, dict):
            data = [dict(zip(data.keys(), values)) for values in zip(*data.values())]

        return spark.createDataFrame(data)

    def _read_polars(self) -> PolarsLazyFrame:
        logger.info(f"Reading {self._id} from memory")

        import polars as pl

        if self.df is not None:
            return self.df

        return pl.LazyFrame(self.data)
