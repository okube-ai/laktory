from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.spark import SparkDataFrame
from laktory.spark import is_spark_dataframe
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.polars import PolarsDataFrame
from laktory.polars import is_polars_dataframe
from laktory.types import AnyDataFrame
from laktory._logger import get_logger

logger = get_logger(__name__)


class DataFrameSample(BaseModel):
    fraction: float
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
    Base class for building data source

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
    broadcast: Union[bool, None] = False
    dataframe_backend: Literal["SPARK", "POLARS"] = None
    drops: Union[list, None] = None
    filter: Union[str, None] = None
    limit: Union[int, None] = None
    mock_df: Any = Field(default=None, exclude=True)
    renames: Union[dict[str, str], None] = None
    sample: Union[DataFrameSample, None] = None
    selects: Union[list[str], dict[str, str], None] = None
    watermark: Union[Watermark, None] = None

    @model_validator(mode="after")
    def options(self) -> Any:

        # Overwrite Dataframe type if mock dataframe is provided
        if is_spark_dataframe(self.mock_df):
            self.dataframe_backend = "SPARK"
        elif is_polars_dataframe(self.mock_df):
            self.dataframe_backend = "POLARS"

        if self.df_backend == "SPARK":
            pass
        elif self.df_backend == "POLARS":
            if self.as_stream:
                raise ValueError("Polars DataFrames don't support streaming read.")
            if self.watermark:
                raise ValueError("Polars DataFrames don't support watermarking.")
            if self.broadcast:
                raise ValueError("Polars DataFrames don't support broadcasting.")

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        return str(self.df)

    @property
    def is_orchestrator_dlt(self) -> bool:
        """If `True`, data source is used in the context of a DLT pipeline"""

        pl = self.parent_pipeline

        if pl is None:
            return False

        return pl.is_orchestrator_dlt

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def read(self, spark=None) -> AnyDataFrame:
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
        if self.mock_df is not None:
            df = self.mock_df
        elif self.df_backend == "SPARK":
            df = self._read_spark(spark=spark)
        elif self.df_backend == "POLARS":
            df = self._read_polars()
        else:
            raise ValueError(f"DataFrame type '{self.df_backend}' is not supported.")

        if is_spark_dataframe(df):
            df = self._post_read_spark(df)
        elif is_polars_dataframe(df):
            df = self._post_read_polars(df)

        logger.info("Read completed.")

        return df

    def _read_spark(self, spark) -> SparkDataFrame:
        raise NotImplementedError()

    def _read_polars(self) -> PolarsDataFrame:
        raise NotImplementedError()

    def _post_read_spark(self, df: SparkDataFrame) -> SparkDataFrame:

        import pyspark.sql.functions as F

        # Apply filter
        if self.filter:
            df = df.filter(self.filter)

        # Apply drops
        if self.drops:
            df = df.drop(*self.drops)

        # Columns
        cols = []
        if self.selects:
            if isinstance(self.selects, list):
                cols += [F.col(c) for c in self.selects]
            elif isinstance(self.selects, dict):
                cols += [F.col(k).alias(v) for k, v in self.selects.items()]
            df = df.select(cols)

        # Renames
        if self.renames:
            for old_name, new_name in self.renames.items():
                df = df.withColumnRenamed(old_name, new_name)

        # Apply Watermark
        if self.watermark:
            df = df.withWatermark(
                self.watermark.column,
                self.watermark.threshold,
            )

        # Broadcast
        if self.broadcast:
            df = F.broadcast(df)

        # Sample
        if self.sample:
            df = df.sample(fraction=self.sample.fraction, seed=self.sample.seed)

        # Limit
        if self.limit:
            df = df.limit(self.limit)

        return df

    def _post_read_polars(self, df: PolarsDataFrame) -> PolarsDataFrame:

        from laktory.polars.expressions.sql import _parse_token
        import polars as pl

        # Apply filter
        if self.filter:
            df = df.filter(pl.Expr.laktory.sql_expr(self.filter))

        # Columns
        cols = []
        if self.selects:
            if isinstance(self.selects, list):
                cols += [_parse_token(c) for c in self.selects]
            elif isinstance(self.selects, dict):
                cols += [_parse_token(k).alias(v) for k, v in self.selects.items()]
            df = df.select(cols)

        # Apply drops
        if self.drops:
            df = df.drop(*self.drops)

        # Renames
        if self.renames:
            df = df.rename(self.renames)

        # Apply Watermark
        if self.watermark:
            raise NotImplementedError(
                "Watermarking not supported with POLARS dataframe"
            )

        # Broadcast
        if self.broadcast:
            raise NotImplementedError(
                "Broadcasting not supported with POLARS dataframe"
            )

        # Sample
        if self.sample:
            raise NotImplementedError(
                "Broadcasting not supported with POLARS lazyframe"
            )
            # df = df.sample(fraction=self.sample.fraction, seed=self.sample.seed)

        # Limit
        if self.limit:
            df = df.limit(self.limit)

        return df
