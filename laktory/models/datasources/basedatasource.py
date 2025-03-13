from typing import Any
from typing import Union

import narwhals as nw
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.pipeline.pipelinechild import PipelineChild

logger = get_logger(__name__)


AnyFrame = Union[nw.LazyFrame, nw.DataFrame]


class DataFrameSample(BaseModel):
    n: int = None
    fraction: float = None
    seed: Union[int, None] = None


class Watermark(BaseModel):
    """
    Definition of a spark structured streaming watermark for joining data
    streams.

    Attributes
    ----------
    column:
        Event time column name
    threshold:
        How late, expressed in seconds, the data is expected to be with
        respect to event time.

    References
    ----------
    https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking
    """

    column: str
    threshold: str


class BaseDataSource(BaseModel, PipelineChild):
    """
    Base class for data sources.

    Attributes
    ----------
    as_stream:
        If `True`source is read as a streaming DataFrame.
    broadcast:
        If `True` DataFrame is broadcasted
    dataframe_backend:
        Type of dataframe
    drops:
        List of columns to drop
    filter:
        SQL expression used to select specific rows from the source table
    renames:
        Mapping between the source table column names and new column names
    selects:
        Columns to select from the source table. Can be specified as a list
        or as a dictionary to rename the source columns
    watermark
        Spark structured streaming watermark specifications
    """

    as_stream: bool = False
    # broadcast: Union[bool, None] = False
    dataframe_backend: DataFrameBackends = None
    drop_duplicates: Union[bool, list[str]] = None
    drops: list = None
    filter: str = None
    renames: dict[str, str] = None
    # sample: DataFrameSample = None
    selects: Union[list[str], dict[str, str]] = None
    # watermark: Union[Watermark, None] = None
    type: str

    @model_validator(mode="after")
    def options(self) -> Any:
        with self.validate_assignment_disabled():
            if self.df_backend == DataFrameBackends.PYSPARK:
                pass
            elif self.df_backend == DataFrameBackends.POLARS:
                if self.as_stream:
                    raise ValueError("Polars DataFrames don't support streaming read.")
                # if self.watermark:
                #     raise ValueError("Polars DataFrames don't support watermarking.")
                # if self.broadcast:
                #     raise ValueError("Polars DataFrames don't support broadcasting.")

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        raise NotImplementedError()

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def read(self, spark=None) -> AnyFrame:
        """
        Read data with options specified in attributes.

        Parameters
        ----------
        spark:
            Spark context

        Returns
        -------
        : DataFrame
            Resulting dataframe
        """
        logger.info(
            f"Reading `{self.__class__.__name__}` {self._id} with {self.df_backend}"
        )
        df = self._read(spark=spark)

        # Convert to Narwhals
        if not isinstance(df, (nw.LazyFrame, nw.DataFrame)):
            df = nw.from_native(df)

        # Post read
        df = self._post_read(df)

        logger.info("Read completed.")

        return df

    def _read(self, spark=None) -> AnyFrame:
        if self.df_backend == DataFrameBackends.PYSPARK:
            return self._read_spark(spark=spark)

        if self.df_backend == DataFrameBackends.POLARS:
            return self._read_polars()

        raise ValueError(f"`{self.df_backend}` not supported for `{type(self)}`")

    def _read_spark(self, spark=None) -> nw.LazyFrame:
        raise NotImplementedError(
            f"`{self.df_backend}` not supported for `{type(self)}`"
        )

    def _read_polars(self) -> nw.LazyFrame:
        raise NotImplementedError(
            f"`{self.df_backend}` not supported for `{type(self)}`"
        )

    def _post_read(self, df: AnyFrame) -> AnyFrame:
        # Apply filter
        if self.filter:
            df = df.filter(nw.sql_expr(self.filter))

        # Columns
        cols = []
        if self.selects:
            if isinstance(self.selects, list):
                cols += [nw.sql_expr(c) for c in self.selects]
            elif isinstance(self.selects, dict):
                cols += [nw.sql_expr(k).alias(v) for k, v in self.selects.items()]
            df = df.select(cols)

        # Renames
        if self.renames:
            df = df.rename(self.renames)

        # Apply drops
        if self.drops:
            df = df.drop(*self.drops, strict=False)

        # # Apply Watermark
        # if self.watermark:
        #     df = df.withWatermark(
        #         self.watermark.column,
        #         self.watermark.threshold,
        #     )
        #
        # # Broadcast
        # if self.broadcast:
        #     df = F.broadcast(df)

        # Drop Duplicates
        if self.drop_duplicates:
            subset = None
            if isinstance(self.drop_duplicates, list):
                subset = self.drop_duplicates

            df = df.unique(subset=subset)

        # Sample
        # TODO: Enable when Narwhals support sampling on LazyFrame
        # if self.sample:
        #     df = df.sample(
        #         n=self.sample.n, fraction=self.sample.fraction, seed=self.sample.seed
        #     )

        return df
