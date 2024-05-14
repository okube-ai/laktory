from typing import Union
from typing import Any
from typing import Literal
from pydantic import Field
from laktory.models.basemodel import BaseModel
from laktory.spark import is_spark_dataframe
from laktory.spark import SparkDataFrame
from laktory.polars import is_polars_dataframe
from laktory.polars import PolarsDataFrame
from laktory.types import AnyDataFrame


class BaseDataSink(BaseModel):
    """
    Base class for building data sink

    Attributes
    ----------
    as_stream:
        If `True`DataFrame is written as a data stream.
    mode:
        Write mode.
        - overwrite: Overwrite existing data
        - append: Append contents of the DataFrame to existing data
        - error: Throw and exception if data already exists
        - ignore: Silently ignore this operation if data already exists
    """

    as_stream: bool = False
    mode: Union[Literal["OVERWRITE", "APPEND", "IGNORE", "ERROR"], None] = None

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        return str(self)

    # ----------------------------------------------------------------------- #
    # Writers                                                                 #
    # ----------------------------------------------------------------------- #

    def write(self, df: AnyDataFrame, mode=None) -> None:
        if mode is None:
            mode = self.mode
        if is_spark_dataframe(df):
            self._write_spark(df, mode=mode)
        elif is_polars_dataframe(df):
            if self.as_stream:
                raise ValueError("Polars DataFrames don't support streaming write.")
            self._write_polars(df, mode=mode)
        else:
            raise ValueError()

    def _write_spark(self, df: SparkDataFrame, mode=mode) -> None:
        raise NotImplementedError("Not implemented for Spark DataFrame")

    def _write_polars(self, df: PolarsDataFrame, mode=mode) -> None:
        raise NotImplementedError("Not implemented for Polars DataFrame")
