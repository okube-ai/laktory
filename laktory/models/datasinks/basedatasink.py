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
    mode:
        Write mode.
        - overwrite: Overwrite existing data
        - append: Append contents of the dataframe to existing data
        - error: Throw and exception if data already exists
        - ignore: Silently ignore this operation if data already exists
        - complete: Overwrite for streaming dataframes
    """

    mode: Union[Literal["OVERWRITE", "APPEND", "IGNORE", "ERROR", "COMPLETE"], None] = (
        None
    )
    _parent: "PipelineNode" = None

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
        """
        Write dataframe into sink.

        Parameters
        ----------
        df:
            Input dataframe
        mode:
            Write mode overwrite of the sink default mode.

        Returns
        -------
        """
        if mode is None:
            mode = self.mode
        if is_spark_dataframe(df):
            self._write_spark(df, mode=mode)
        elif is_polars_dataframe(df):
            self._write_polars(df, mode=mode)
        else:
            raise ValueError()

    def _write_spark(self, df: SparkDataFrame, mode=mode) -> None:
        raise NotImplementedError("Not implemented for Spark dataframe")

    def _write_polars(self, df: PolarsDataFrame, mode=mode) -> None:
        raise NotImplementedError("Not implemented for Polars dataframe")

    # ----------------------------------------------------------------------- #
    # Sources                                                                 #
    # ----------------------------------------------------------------------- #

    def as_source(self, as_stream=None):
        raise NotImplementedError()

    def read(self, spark=None, as_stream=None):
        """
        Read dataframe from sink.

        Parameters
        ----------
        spark:
            Spark Session
        as_stream:
            If `True`, dataframe read as stream.

        Returns
        -------
        """
        return self.as_source(as_stream=as_stream).read(spark=spark)
