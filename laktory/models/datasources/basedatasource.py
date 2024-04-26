from abc import abstractmethod
from typing import Any
from typing import Union
from laktory.spark import DataFrame
from pydantic import Field

from laktory.models.basemodel import BaseModel


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


class BaseDataSource(BaseModel):
    """
    Base class for building data source

    Attributes
    ----------
    drops:
        List of columns to drop
    selects:
        Columns to select from the source table. Can be specified as a list
        or as a dictionary to rename the source columns
    filter:
        SQL expression used to select specific rows from the source table
    read_as_stream
        If `True` read source as stream
    renames:
        Mapping between the source table column names and new column names
    watermark
        Spark structured streaming watermark specifications

    """

    drops: Union[list, None] = None
    filter: Union[str, None] = None
    mock_df: Any = Field(default=None, exclude=True)
    read_as_stream: Union[bool, None] = True
    renames: Union[dict[str, str], None] = None
    selects: Union[list[str], dict[str, str], None] = None
    watermark: Union[Watermark, None] = None

    def read(self, spark) -> DataFrame:
        """
        Read data with options specified in attributes.

        Parameters
        ----------
        spark
            Spark context

        Returns
        -------
        : DataFrame
            Resulting park dataframe
        """
        df = self._read(spark)
        df = self._post_read(df)
        return df

    @abstractmethod
    def _read(self, spark) -> DataFrame:
        raise NotImplementedError()

    def _post_read(self, df) -> DataFrame:
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

        return df

    @property
    def is_cdc(self) -> bool:
        """If `True` source data is a change data capture (CDC)"""
        return getattr(self, "cdc", None) is not None
