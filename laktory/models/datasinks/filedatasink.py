import os
import shutil
from typing import Literal
from typing import Union
from laktory.models.datasinks.basedatasink import BaseDataSink
from laktory.spark import SparkDataFrame
from laktory.polars import PolarsDataFrame
from laktory.models.datasources.filedatasource import FileDataSource
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
    )
    # sink.write(df)
    ```
    """

    checkpoint_location: Union[str, None] = None
    format: Literal["CSV", "PARQUET", "DELTA", "JSON", "EXCEL"] = "DELTA"
    path: str

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
        return os.path.join(os.path.dirname(self.path), "checkpoint/")

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def _write_spark(self, df: SparkDataFrame, mode=None) -> None:

        if self.format in ["EXCEL"]:
            raise ValueError(f"'{self.format}' format is not supported with Spark")

        # Set mode
        if mode is None:
            mode = self.mode

        # Default Options
        _options = {"mergeSchema": "true", "overwriteSchema": "false"}
        if mode in ["OVERWRITE", "COMPLETE"]:
            _options["mergeSchema"] = "false"
            _options["overwriteSchema"] = "true"
        if df.isStreaming:
            _options["checkpointLocation"] = self._checkpoint_location

        # User Options
        for k, v in self.write_options.items():
            _options[k] = v

        t = "static"
        if df.isStreaming:
            t = "stream"
            writer = df.writeStream.trigger(
                availableNow=True
            )  # TODO: Add option for trigger?

        else:
            writer = df.write

        logger.info(
            f"Writing df as {t} {self.format} to {self.path} with mode {mode} and options {_options}"
        )
        writer.mode(mode).format(self.format).options(**_options).save(self.path)

    def _write_polars(self, df: PolarsDataFrame, mode=None) -> None:

        isStreaming = False

        if isStreaming:
            pass
            logger.info(f"Writing df as stream {self.format} to {self.path}")

        else:
            logger.info(f"Writing df as static {self.format} to {self.path}")

        if self.format != "DELTA":
            if mode:
                raise ValueError(
                    "'mode' configuration with Polars only supported by 'DELTA' format"
                )
        else:
            if not mode:
                raise ValueError(
                    "'mode' configuration required with Polars 'DELTA' format"
                )

        if self.format == "CSV":
            df.write_csv(self.path, **self.write_options)
        elif self.format == "DELTA":
            df.write_delta(self.path, mode=mode, **self.write_options)
        elif self.format == "EXCEL":
            df.write_excel(self.path, **self.write_options)
        elif self.format == "JSON":
            df.write_json(self.path, **self.write_options)
        elif self.format == "PARQUET":
            df.write_parquet(self.path, **self.write_options)

    # ----------------------------------------------------------------------- #
    # Purge                                                                   #
    # ----------------------------------------------------------------------- #

    def purge(self, spark=None):
        """
        Delete sink data and checkpoints
        """
        # Remove Data
        if os.path.exists(self.path):
            is_dir = os.path.isdir(self.path)
            if is_dir:
                logger.info(f"Deleting data dir {self.path}")
                shutil.rmtree(self.path)
            else:
                logger.info(f"Deleting data file {self.path}")
                os.remove(self.path)

        # Remove Checkpoint
        if self._checkpoint_location:
            if os.path.exists(self._checkpoint_location):
                logger.info(
                    f"Deleting checkpoint at {self._checkpoint_location}",
                )
                shutil.rmtree(self._checkpoint_location)

    # ----------------------------------------------------------------------- #
    # Source                                                                  #
    # ----------------------------------------------------------------------- #

    def as_source(self, as_stream: bool = None) -> FileDataSource:
        """
        Generate a file data source with the same path as the sink.

        Parameters
        ----------
        as_stream:
            If `True`, sink will be read as stream.

        Returns
        -------
        :
            File Data Source
        """

        source = FileDataSource(
            path=self.path,
            format=self.format,
        )
        if as_stream:
            source.as_stream = as_stream

        return source
