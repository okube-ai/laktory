import os
import shutil
from pathlib import Path
from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import field_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.datasinks.basedatasink import POLARS_DELTA_MODES
from laktory.models.datasinks.basedatasink import BaseDataSink
from laktory.models.datasources.filedatasource import FileDataSource

SUPPORTED_FORMATS = {
    DataFrameBackends.PYSPARK: [
        "AVRO",
        "BINARYFILE",
        "CSV",
        "DELTA",
        "JSON",
        "JSONL",
        "NDJSON",  # SAME AS JSONL
        "ORC",
        "PARQUET",
        "TEXT",
        "XML",
    ],
    DataFrameBackends.POLARS: [
        "AVRO",
        "CSV",
        "DELTA",
        "EXCEL",
        "IPC",
        # "ICEBERG", # TODO
        "JSON",
        "JSONL",
        "NDJSON",  # SAME AS JSONL
        "PARQUET",
        "PYARROW",
    ],
}

ALL_SUPPORTED_FORMATS = tuple(sorted(set().union(*SUPPORTED_FORMATS.values())))

logger = get_logger(__name__)


class FileDataSink(BaseDataSink):
    """
    Data sink writing to disk file(s) as csv, parquet or Delta Table.

    Examples
    ---------
    Write polars DataFrame as CSV
    ```python
    import polars as pl

    import laktory as lk

    df = pl.DataFrame({"x": [0, 1]})

    sink = lk.models.FileDataSink(
        path="./dataframe.csv", format="CSV", writer_kwargs={"separator": ";"}
    )
    sink.write(df)
    ```

    Write Spark Streaming DataFrame as Delta
    ```python tag:skip-run
    from laktory import models

    df = spark.readStream(...)  # skip

    sink = models.FileDataSink(
        path="./delta_table/",
        format="DELTA",
        mode="APPEND",
        checkpoint_path="./delta_table/checkpoint/",
    )
    sink.write(df)
    ```
    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/sourcessinks/)
    """

    format: Literal.__getitem__(ALL_SUPPORTED_FORMATS) = Field(
        ..., description="Format of the data files."
    )
    path: str = Field(
        ...,
        description="File path on a local disk, remote storage or Databricks volume.",
    )
    type: Literal["FILE"] = Field("FILE", frozen=True, description="Source Type")

    @field_validator("path", mode="before")
    @classmethod
    def posixpath_to_string(cls, path: Any) -> Any:
        if isinstance(path, Path):
            path = str(path)
        return path

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        return self.path

    # ----------------------------------------------------------------------- #
    # Writers                                                                 #
    # ----------------------------------------------------------------------- #

    def _validate_format(self) -> None:
        if self.format not in SUPPORTED_FORMATS[self.dataframe_backend]:
            raise ValueError(
                f"'{self.format}' format is not supported with {self.dataframe_backend}. Use one of {SUPPORTED_FORMATS[self.dataframe_backend]}"
            )
        if self.mode == "MERGE" and self.format != "DELTA":
            raise ValueError("Only 'DELTA' format is supported with 'MERGE' mode.")

    def _validate_mode_polars(self, mode, df):
        if self.format == "DELTA":
            if mode not in POLARS_DELTA_MODES:
                raise ValueError(
                    f"Mode '{mode}' is not supported for Polars DataFrame with DELTA format. Set to {POLARS_DELTA_MODES}"
                )
        else:
            if mode:
                raise ValueError(
                    f"Mode '{mode}' is not supported for Polars DataFrame. Set to `None`"
                )

    def _write_spark(self, df, mode, full_refresh=False) -> None:
        df = df.to_native()

        # Format
        methods = self._get_spark_writer_methods(mode=mode, is_streaming=df.isStreaming)

        if df.isStreaming:
            logger.info(
                f"Writing df to {self.path} with writeStream.{'.'.join([m.as_string for m in methods])}"
            )

            writer = df.writeStream
            for m in methods:
                writer = getattr(writer, m.name)(*m.args, **m.kwargs)

            query = writer.start(self.path)
            query.awaitTermination()

        else:
            logger.info(
                f"Writing df to {self.path} with write.{'.'.join([m.as_string for m in methods])}"
            )
            writer = df.write
            for m in methods:
                writer = getattr(writer, m.name)(*m.args, **m.kwargs)

            writer.save(self.path)

    def _write_polars(self, df, mode, full_refresh=False) -> None:
        import polars as pl

        kwargs, fmt = self._get_polars_kwargs(mode=mode)

        df = df.to_native()

        is_streaming = False

        _mode = "streaming" if is_streaming else "static"
        logger.info(
            f"Writing {_mode} df to {self.path} with format '{self.format}' and {kwargs}"
        )

        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        if self.format.lower() == "avro":
            df.write_avro(self.path, **self.writer_kwargs)
        if self.format.lower() == "csv":
            df.write_csv(self.path, **self.writer_kwargs)
        elif self.format.lower() == "delta":
            df.write_delta(self.path, mode=mode, **self.writer_kwargs)
        elif self.format.lower() == "excel":
            df.write_excel(self.path, **self.writer_kwargs)
        elif self.format.lower() == "ipc":
            df.write_ipc(self.path, **self.writer_kwargs)
        elif self.format.lower() == "json":
            df.write_json(self.path, **self.writer_kwargs)
        elif self.format.lower() in ["ndjson", "jsonl"]:
            df.write_ndjson(self.path, **self.writer_kwargs)
        elif self.format.lower() == "parquet":
            df.write_parquet(self.path, **self.writer_kwargs)
        elif self.format.lower() == "pyarrow":
            import pyarrow.dataset as ds

            kwargs = {"format": "parquet", "partitioning": None}
            for k, v in self.writer_kwargs.items():
                kwargs[k] = v
            ds.write_dataset(data=df.to_arrow(), base_dir=self.path, **kwargs)

    # ----------------------------------------------------------------------- #
    # Purge                                                                   #
    # ----------------------------------------------------------------------- #

    def purge(self):
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

        # TODO: Add support for Databricks dbfs / workspace / Volume?

        # Remove Checkpoint
        self._purge_checkpoint()

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
            path=self.path, format=self.format, dataframe_backend=self.dataframe_backend
        )

        if as_stream:
            source.as_stream = as_stream

        # if self.dataframe_backend:
        #     source.dataframe_backend = self.dataframe_backend
        source.parent = self.parent

        return source
