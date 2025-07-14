from typing import Any
from typing import Literal

import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild
from laktory.narwhals_ext.functions.sql_expr import sql_expr
from laktory.typing import AnyFrame

logger = get_logger(__name__)


class DataFrameSample(BaseModel):
    n: int = None
    fraction: float = None
    seed: int | None = None


class Watermark(BaseModel):
    """
    References
    ----------
    https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking
    """

    column: str = Field(..., description="Event time column name")
    threshold: str = Field(
        ...,
        description="How late, expressed in seconds, the data is expected to be with respect to event time.",
    )


class BaseDataSource(BaseModel, PipelineChild):
    """
    Base class for data sources.
    """

    as_stream: bool = Field(
        False,
        description="If `True`source is read as a streaming DataFrame. Currently only supported by Spark DataFrame backend.",
    )
    # broadcast: bool = Field(
    #         False, description="If `True` DataFrame is broadcasted."
    #     )
    drop_duplicates: bool | list[str] = Field(
        None,
        description="Remove duplicated rows from source using all columns if `True` or only the provided column names.",
    )
    drops: list = Field(
        None,
        description="List of columns to drop",
    )
    filter: str = Field(
        None,
        description="SQL expression used to select specific rows from the source table",
    )
    renames: dict[str, str] = Field(
        None,
        description="Mapping between the source column names and desired column names",
    )
    # sample: DataFrameSample = None
    selects: list[str] | dict[str, str] = Field(
        None,
        description="Columns to select from the source. Can be specified as a list or as a dictionary to rename the source columns",
    )
    # watermark: Watermark | None = Field(None, description="Spark structured streaming watermark specifications")
    type: Literal["DATAFRAME", "FILE", "UNITY_CATALOG", "HIVE_METASTORE"] = Field(
        ..., description="Name of the data source type"
    )

    @model_validator(mode="after")
    def options(self) -> Any:
        with self.validate_assignment_disabled():
            if self.dataframe_backend == DataFrameBackends.PYSPARK:
                pass
            elif self.dataframe_backend == DataFrameBackends.POLARS:
                if self.as_stream:
                    raise ValueError(
                        "Streaming read is not supported with Polars Backend."
                    )
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

    def read(self, **kwargs) -> AnyFrame:
        """
        Read data with options specified in attributes.

        Returns
        -------
        :
            Resulting dataframe
        """
        logger.info(
            f"Reading `{self.__class__.__name__}` {self._id} with {self.dataframe_backend}"
        )
        df = self._read(**kwargs)

        # Convert to Narwhals
        if not isinstance(df, (nw.LazyFrame, nw.DataFrame)):
            df = nw.from_native(df)

        # Post read
        df = self._post_read(df)

        logger.info("Read completed.")

        return df

    def _read(self, **kwargs) -> AnyFrame:
        if self.dataframe_backend == DataFrameBackends.PYSPARK:
            return self._read_spark()

        if self.dataframe_backend == DataFrameBackends.POLARS:
            return self._read_polars(**kwargs)

        raise ValueError(f"`{self.dataframe_backend}` not supported for `{type(self)}`")

    def _read_spark(self, **kwargs) -> nw.LazyFrame:
        raise NotImplementedError(
            f"`{self.dataframe_backend}` not supported for `{type(self)}`"
        )

    def _read_polars(self, **kwargs) -> nw.LazyFrame:
        raise NotImplementedError(
            f"`{self.dataframe_backend}` not supported for `{type(self)}`"
        )

    def _post_read(self, df: AnyFrame) -> AnyFrame:
        # Apply filter
        if self.filter:
            df = df.filter(sql_expr(self.filter))

        # Columns
        cols = []
        if self.selects:
            if isinstance(self.selects, list):
                cols += [sql_expr(c) for c in self.selects]
            elif isinstance(self.selects, dict):
                cols += [sql_expr(k).alias(v) for k, v in self.selects.items()]
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
