from pathlib import Path
from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import field_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.datasinks.basedatasink import POLARS_DELTA_MODES
from laktory.models.datasinks.basedatasink import BaseDataSink

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

        # if mode.lower() == "merge":
        #     self.merge_cdc_options.execute(source=df)
        #     return

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

        # Default Options
        _options = {"mergeSchema": "true", "overwriteSchema": "false"}
        if mode in ["OVERWRITE", "COMPLETE"]:
            _options["mergeSchema"] = "false"
            _options["overwriteSchema"] = "true"
        if df.isStreaming:
            _options["checkpointLocation"] = self._checkpoint_location

        # Format
        fmt = self.format.lower()
        if fmt in ["jsonl", "ndjson"]:
            fmt = "json"
            _options["multiline"] = False
        elif fmt in ["json"]:
            _options["multiline"] = True

        # User Options
        for k, v in self.writer_kwargs.items():
            _options[k] = v

        if df.isStreaming:
            logger.info(
                f"Writing df as stream {self.format} to {self.path} with mode {mode} and options {_options}"
            )
            writer = (
                df.writeStream.format(fmt)
                .outputMode(mode)
                .trigger(availableNow=True)  # TODO: Add option for trigger?
                .options(**_options)
            )

            # Apply methods
            for m in self.writer_methods:
                writer = getattr(writer, m.name)(*m.args, **m.kwargs)

            query = writer.start(self.path)
            query.awaitTermination()

        else:
            logger.info(
                f"Writing df as static {self.format} to {self.path} with mode {mode} and options {_options}"
            )
            writer = df.write.mode(mode).format(fmt).options(**_options)

            # Apply methods
            for m in self.writer_methods:
                writer = getattr(writer, m.name)(*m.args, **m.kwargs)

            writer.save(self.path)

    def _write_polars(self, df, mode, full_refresh=False) -> None:
        from polars import LazyFrame

        df = df.to_native()

        isStreaming = False

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

        if isStreaming:
            logger.info(
                f"Writing df as stream {self.format} to {self.path} with mode {mode}"
            )

        else:
            logger.info(
                f"Writing df as static {self.format} to {self.path} with mode {mode}"
            )

        if isinstance(df, LazyFrame):
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
