from pathlib import Path
from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import field_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.datasinks.basedatasink import POLARS_DELTA_MODES
from laktory.models.datasinks.basedatasink import BaseDataSink
from laktory.models.datasinks.basedatasink import WriterMethod

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
    format: Literal.__getitem__(ALL_SUPPORTED_FORMATS) = Field(
        ..., description="Format of the data files."
    )
    path: str = Field(
        ...,
        description="File path on a local disk, remote storage or Databricks volume.",
    )
    type: Literal["FILE"] = Field("FILE", frozen=True, description="Source Type")
    writer_kwargs: dict[str, Any] = Field(
        {},
        description="Keyword arguments passed directly to dataframe backend writer."
        "Passed to `.options()` method when using PySpark.",
    )

    @field_validator("path", mode="before")
    @classmethod
    def posixpath_to_string(cls, path: Any) -> Any:
        if isinstance(path, Path):
            path = str(path)
        return path

    # ----------------------------------------------------------------------- #
    # Writers                                                                 #
    # ----------------------------------------------------------------------- #

    def _validate_format(self) -> None:
        if self.format not in SUPPORTED_FORMATS[self.df_backend]:
            raise ValueError(
                f"'{self.format}' format is not supported with {self.dataframe_backend}. Use one of {SUPPORTED_FORMATS[self.df_backend]}"
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

    def _get_spark_kwargs(self, mode, is_streaming):
        fmt = self.format.lower()

        # Build kwargs
        kwargs = {}

        kwargs["mergeSchema"] = True
        kwargs["overwriteSchema"] = False
        if mode in ["OVERWRITE", "COMPLETE"]:
            kwargs["mergeSchema"] = False
            kwargs["overwriteSchema"] = True
        if is_streaming:
            kwargs["checkpointLocation"] = self._checkpoint_location

        if fmt in ["jsonl", "ndjson"]:
            fmt = "json"
            kwargs["multiline"] = False
        elif fmt in ["json"]:
            kwargs["multiline"] = True

        # User Options
        for k, v in self.writer_kwargs.items():
            kwargs[k] = v

        return kwargs, fmt

    def _get_spark_writer_methods(self, mode, is_streaming):
        methods = []

        options, fmt = self._get_spark_kwargs(mode=mode, is_streaming=is_streaming)

        methods += [WriterMethod(name="format", args=[fmt])]

        if is_streaming:
            methods += [WriterMethod(name="outputMode", args=[mode])]
            methods += [WriterMethod(name="trigger", kwargs={"availableNow": True})]
        else:
            methods += [WriterMethod(name="mode", args=[mode])]

        if options:
            methods += [WriterMethod(name="options", kwargs=options)]

        for m in self.writer_methods:
            methods += [m]

        return methods

    def _write_spark(self, df, mode, full_refresh=False) -> None:
        df = df.to_native()

        # Full Refresh
        # if full_refresh or not self.exists(spark=df.sparkSession):
        #     if df.isStreaming:
        #         pass
        #         # .is_aggregate() method seems unreliable. Disabling for now.
        #         # if df.laktory.is_aggregate():
        #         #     logger.info(
        #         #         "Full refresh or initial load. Switching to COMPLETE mode."
        #         #     )
        #         #     mode = "COMPLETE"
        #     else:
        #         logger.info(
        #             "Full refresh or initial load. Switching to OVERWRITE mode."
        #         )
        #         mode = "OVERWRITE"

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

    def _get_polars_kwargs(self, mode):
        fmt = self.format.lower()

        kwargs = {}

        if self.format != "DELTA":
            if mode:
                raise ValueError(
                    "'mode' configuration with Polars only supported by 'DELTA' format"
                )
        else:
            # if full_refresh or not self.exists():
            #     mode = "OVERWRITE"

            if not mode:
                raise ValueError(
                    "'mode' configuration required with Polars 'DELTA' format"
                )

        if mode:
            kwargs["mode"] = mode

        for k, v in self.writer_kwargs.items():
            kwargs[k] = v

        return kwargs, fmt

    def _write_polars(self, df, mode, full_refresh=False) -> None:
        import polars as pl

        kwargs, fmt = self._get_polars_kwargs(mode=mode)

        df = df.to_native()

        is_streaming = False

        _mode = "static" if is_streaming else "streaming"
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
