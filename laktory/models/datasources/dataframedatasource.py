from typing import Any
from typing import Literal

import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.datasources.basedatasource import BaseDataSource

logger = get_logger(__name__)

AnyFrame = nw.DataFrame | nw.LazyFrame


class DataFrameDataSource(BaseDataSource):
    """
    Data source using in-memory DataFrame.

    Examples
    ---------
    From data with PySpark backend.
    ```python
    import laktory as lk

    data = {
        "x": [0, 1],
        "y": ["a", "b"],
    }

    # From data using PySpark
    source = lk.models.DataFrameDataSource(
        data=data,
        dataframe_backend="PYSPARK",
    )
    df = source.read()
    print(df.collect(backend="pandas"))
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |        x  y      |
    |     0  0  a      |
    |     1  1  b      |
    └──────────────────┘
    '''
    ```

    From Polars DataFrame
    ```python
    import polars as pl

    import laktory as lk

    data = {
        "x": [0, 1],
        "y": ["a", "b"],
    }

    source = lk.models.DataFrameDataSource(
        df=pl.DataFrame(data),
    )
    df = source.read()
    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |    | x | y |     |
    |    |---|---|     |
    |    | 0 | a |     |
    |    | 1 | b |     |
    └──────────────────┘
    '''
    ```
    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/sourcessinks/)
    """

    data: dict[str, list[Any]] | list[dict[str, Any]] = Field(
        None, description="Serialized data used to build source"
    )
    df: Any = Field(None, description="DataFrame object acting as source")
    type: Literal["DATAFRAME"] = Field(
        "DATAFRAME",
        frozen=True,
        description="Source Type",
    )

    @model_validator(mode="after")
    def validate_input(self) -> Any:
        if self.df is None and self.data is None:
            raise ValueError("Either `data` or `df` must be provided.")

        if self.df is not None and self.data is not None:
            raise ValueError("Only `data` or `df` can be provided.")

        if self.df is not None:
            if not isinstance(self.df, (nw.DataFrame, nw.LazyFrame)):
                self.df = nw.from_native(self.df)

            with self.validate_assignment_disabled():
                self.dataframe_backend_ = DataFrameBackends.from_nw_implementation(
                    self.df.implementation
                )

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        n = 4

        # Build Schema-like
        if self.df is not None:
            d = self.df.schema
        elif isinstance(self.data, dict):
            d = {k: str(type(v)) for k, v in self.data.items()}
        elif isinstance(self.data, list):
            d = {k: str(type(v)) for k, v in self.data[0].items()}
        else:
            d = {}

        # Build id
        _id = "DataFrame[" + ", ".join([f"{k}: {v}" for k, v in d.items()][:n]) + "]"
        if len(d) >= n:
            _id = _id.replace("]", "...]")

        if isinstance(self, nw.LazyFrame):
            _id = _id.replace("DataFrame", "LazyFrame")

        return _id

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_spark(self) -> nw.LazyFrame:
        from laktory import get_spark_session

        spark = get_spark_session()

        if self.df is not None:
            return nw.from_native(self.df)

        data = self.data
        if isinstance(data, dict):
            data = [dict(zip(data.keys(), values)) for values in zip(*data.values())]

        df = spark.createDataFrame(data)

        return nw.from_native(df)

    def _read_polars(self) -> AnyFrame:
        if self.df is not None:
            return nw.from_native(self.df)

        import polars as pl

        df = pl.LazyFrame(self.data)

        return nw.from_native(df)
