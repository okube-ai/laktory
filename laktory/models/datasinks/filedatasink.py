import os
from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator
from laktory.models.datasinks.basedatasink import BaseDataSink
from laktory.spark import SparkDataFrame
from laktory._logger import get_logger

logger = get_logger(__name__)


class FileDataSink(BaseDataSink):
    """
    Disk file(s) data sink such as csv, parquet or Delta Table.

    Attributes
    ----------
    checkpoint_location:
        Path to which the checkpoint file for streaming dataframe should
        be written. If `None`, parent directory of `path` is used.
    format:
        Format of the data files
    path:
        Path to which the DataFrame needs to be written.
    write_options:
        Other options passed to `spark.read.options`

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

    sink = models.FileDataSink(
        path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
        format="PARQUET",
        mode="OVERWRITE",
        as_stream=False,
    )
    # sink.write(df)
    ```
    """

    checkpoint_location: Union[str, None] = None
    format: Literal["CSV", "PARQUET", "DELTA", "JSON"] = "DELTA"
    path: str
    write_options: dict[str, str] = {}

    @model_validator(mode="after")
    def check_format(self) -> Any:
        if self.as_stream and self.format != "DELTA":
            raise ValueError("Streaming is only supported with Delta")

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        return str(self.path)

    @property
    def _checkpoint_location(self):
        if self.checkpoint_location:
            return self.checkpoint_location
        return os.path.dirname(self.path)

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def _write_spark(self, df: SparkDataFrame, mode=None) -> None:

        if self.as_stream:
            logger.info(f"Writing df as stream to {self.path}")

            writer = df.writeStream.option(
                "checkpointlocation", self._checkpoint_location
            )

        else:
            logger.info(f"Writing df as static to {self.path}")
            writer = df.write

        writer = writer.mode(mode).format(self.format)

        if self.write_options:
            writer = writer.options(**self.write_options)

        writer.save(self.path)
