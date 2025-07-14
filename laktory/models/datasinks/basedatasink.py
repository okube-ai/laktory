import hashlib
import os
import shutil
import uuid
from pathlib import Path
from typing import Any
from typing import Literal

import narwhals as nw
from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field
from pydantic import field_serializer
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.datasinks.mergecdcoptions import DataSinkMergeCDCOptions
from laktory.models.pipelinechild import PipelineChild
from laktory.models.readerwritermethod import ReaderWriterMethod
from laktory.typing import AnyFrame

logger = get_logger(__name__)


SUPPORTED_BACKENDS = [DataFrameBackends.POLARS, DataFrameBackends.PYSPARK]
LAKTORY_MODES = ["MERGE"]
SPARK_MODES = [
    "OVERWRITE",
    "APPEND",
    "IGNORE",
    "ERROR",
    "ERRORIFEXISTS",
] + LAKTORY_MODES
SPARK_STREAMING_MODES = ["APPEND", "COMPLETE", "UPDATE"] + LAKTORY_MODES
POLARS_DELTA_MODES = ["ERROR", "APPEND", "OVERWRITE"] + LAKTORY_MODES
SUPPORTED_MODES = tuple(set(SPARK_MODES + SPARK_STREAMING_MODES + POLARS_DELTA_MODES))


class BaseDataSink(BaseModel, PipelineChild):
    """Base class for data sinks"""

    checkpoint_path_: str | Path = Field(
        None,
        description="Path to which the checkpoint file for which a streaming dataframe should be written.",
        validation_alias=AliasChoices("checkpoint_path", "checkpoint_path_"),
        exclude=True,
    )
    is_quarantine: bool = Field(
        False,
        description="Sink used to store quarantined results from a pipeline node expectations.",
    )
    type: Literal["FILE", "HIVE_METASTORE", "UNITY_CATALOG"] = Field(
        ..., description="Name of the data sink type"
    )
    merge_cdc_options: DataSinkMergeCDCOptions = Field(
        None,
        description="Merge options to handle input DataFrames that are Change Data Capture (CDC). Only used when `MERGE` mode is selected.",
    )  # TODO: Review parameter name
    mode: Literal.__getitem__(SUPPORTED_MODES) | None = Field(
        None,
        description="""
        Write mode.
        
        Spark
        -----
        - OVERWRITE: Overwrite existing data.
        - APPEND: Append contents of this DataFrame to existing data.
        - ERROR: Throw an exception if data already exists.
        - IGNORE: Silently ignore this operation if data already exists.

        Spark Streaming
        ---------------
        - APPEND: Only the new rows in the streaming DataFrame/Dataset will be written to the sink.
        - COMPLETE: All the rows in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
        - UPDATE: Only the rows that were updated in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.

        Polars Delta
        ------------
        - OVERWRITE: Overwrite existing data.
        - APPEND: Append contents of this DataFrame to existing data.
        - ERROR: Throw an exception if data already exists.
        - IGNORE: Silently ignore this operation if data already exists.

        Laktory
        -------
        - MERGE: Append, update and optionally delete records. Only supported for DELTA format. Requires cdc specification.
        """,
    )
    writer_kwargs: dict[str, Any] = Field(
        {},
        description="Keyword arguments passed directly to dataframe backend writer. Passed to `.options()` method when using PySpark.",
    )
    writer_methods: list[ReaderWriterMethod] = Field(
        [], description="DataFrame backend writer methods."
    )

    @model_validator(mode="after")
    def merge_has_options(self) -> Any:
        if self.mode == "MERGE":
            if self.merge_cdc_options is None:
                raise ValueError(
                    "If 'MERGE' `mode` is selected, `merge_cdc_options` must be specified."
                )
            else:
                self.merge_cdc_options._parent = self

        return self

    # -------------------------------------------------------------------------------- #
    # Properties                                                                       #
    # -------------------------------------------------------------------------------- #

    @property
    def _id(self):
        raise NotImplementedError()

    @property
    def _uuid(self) -> str:
        hash_object = hashlib.sha1(self._id.encode())
        hash_digest = hash_object.hexdigest()
        return str(uuid.UUID(hash_digest[:32]))

    @computed_field(description="checkpoint_path")
    @property
    def checkpoint_path(self) -> Path | None:
        if self.checkpoint_path_:
            return Path(self.checkpoint_path_)

        node = self.parent_pipeline_node

        if node and node.root_path:
            for i, s in enumerate(node.all_sinks):
                if s == self:
                    return node.root_path / "checkpoints" / f"sink-{self._uuid}"
        return None

    @field_serializer("checkpoint_path", when_used="json")
    def serialize_path(self, value: Path) -> str:
        return value.as_posix()

    @property
    def upstream_node_names(self) -> list[str]:
        """Pipeline node names required to write sink"""
        return []

    @property
    def data_sources(self):
        """Get all sources feeding the sink"""
        return []

    # -------------------------------------------------------------------------------- #
    # CDC                                                                              #
    # -------------------------------------------------------------------------------- #

    @property
    def is_cdc(self) -> bool:
        return self.merge_cdc_options is not None

    @property
    def dlt_apply_changes_kwargs(self) -> dict[str, str]:
        """Keyword arguments for dlt.apply_changes function"""

        # if not isinstance(self, TableDataSink):
        #     raise ValueError("DLT only supports `TableDataSink` class")

        cdc = self.merge_cdc_options
        return {
            "apply_as_deletes": cdc.delete_where,
            # "apply_as_truncates": ,  # NOT SUPPORTED
            "column_list": cdc.include_columns,
            "except_column_list": cdc.exclude_columns,
            "ignore_null_updates": cdc.ignore_null_updates,
            "keys": cdc.primary_keys,
            "sequence_by": cdc.order_by,
            # "source": self.source.table_name,  # TO SET EXTERNALLY
            "stored_as_scd_type": cdc.scd_type,
            "target": self.table_name,
            # "track_history_column_list": cdc.track_history_columns,  # NOT SUPPORTED
            # "track_history_except_column_list": cdc.track_history_except_columns,  # NOT SUPPORTED
        }

    # -------------------------------------------------------------------------------- #
    # Writers                                                                          #
    # -------------------------------------------------------------------------------- #

    def _validate_format(self):
        return None

    def _validate_mode(self, mode, df):
        if self.dataframe_backend == DataFrameBackends.POLARS:
            self._validate_mode_polars(mode, df)
        elif self.dataframe_backend == DataFrameBackends.PYSPARK:
            self._validate_mode_spark(mode, df)

    def write(
        self, df: AnyFrame = None, mode: str = None, full_refresh: bool = False
    ) -> None:
        """
        Write dataframe into sink.

        Parameters
        ----------
        df:
            Input dataframe.
        full_refresh
            If `True`, source is deleted/dropped (including checkpoint if applicable)
            before write.
        mode:
            Write mode overwrite of the sink default mode.
        """

        if getattr(self, "view_definition", None):
            if self.dataframe_backend == DataFrameBackends.PYSPARK:
                self._write_spark_view()
            elif self.dataframe_backend == DataFrameBackends.POLARS:
                self._write_polars_view()
            else:
                raise ValueError(
                    f"DataFrame backend '{self.dataframe_backend}' is not supported"
                )
            return

        if mode is None:
            mode = self.mode

        if not isinstance(df, (nw.DataFrame, nw.LazyFrame)):
            df = nw.from_native(df)

        dataframe_backend = DataFrameBackends.from_nw_implementation(df.implementation)
        if dataframe_backend not in SUPPORTED_BACKENDS:
            raise ValueError(
                f"DataFrame provided is of {dataframe_backend} backend, which is not supported."
            )

        if self.dataframe_backend_ and self.dataframe_backend_ != dataframe_backend:
            raise ValueError(
                f"DataFrame provided is {dataframe_backend} and source has been configure with {self.dataframe_backend_} backend."
            )
        self.dataframe_backend_ = dataframe_backend

        self._validate_mode(mode, df)
        self._validate_format()

        if mode and mode.lower() == "merge":
            self.merge_cdc_options.execute(source=df)
            logger.info("Write completed.")
            return

        if self.dataframe_backend == DataFrameBackends.PYSPARK:
            self._write_spark(df=df, mode=mode, full_refresh=full_refresh)
        elif self.dataframe_backend == DataFrameBackends.POLARS:
            self._write_polars(df=df, mode=mode, full_refresh=full_refresh)
        else:
            raise ValueError(
                f"DataFrame backend '{self.dataframe_backend}' is not supported"
            )

        logger.info("Write completed.")

    def _write_spark_view(self) -> None:
        raise NotImplementedError(
            f"View creation with spark is not implemented for type '{type(self)}'"
        )

    def _write_polars_view(self) -> None:
        raise NotImplementedError(
            f"View creation with polars is not implemented for type '{type(self)}'"
        )

    # -------------------------------------------------------------------------------- #
    # Writers - Spark                                                                  #
    # -------------------------------------------------------------------------------- #

    def _validate_mode_spark(self, mode, df):
        if df.to_native().isStreaming:
            if mode not in SPARK_STREAMING_MODES:
                raise ValueError(
                    f"Mode '{mode}' is not supported for Spark Streaming DataFrame. Choose from {SPARK_STREAMING_MODES}"
                )
        else:
            if mode not in SPARK_MODES:
                raise ValueError(
                    f"Mode '{mode}' is not supported for Spark DataFrame. Choose from {SPARK_MODES}"
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
            kwargs["checkpointLocation"] = self.checkpoint_path.as_posix()

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

        methods += [ReaderWriterMethod(name="format", args=[fmt])]

        if is_streaming:
            methods += [ReaderWriterMethod(name="outputMode", args=[mode])]
            methods += [
                ReaderWriterMethod(name="trigger", kwargs={"availableNow": True})
            ]
        else:
            methods += [ReaderWriterMethod(name="mode", args=[mode])]

        if options:
            methods += [ReaderWriterMethod(name="options", kwargs=options)]

        for m in self.writer_methods:
            methods += [m]

        return methods

    def _write_spark(self, df, mode, full_refresh) -> None:
        raise NotImplementedError(
            f"`{self.dataframe_backend}` not supported for `{type(self)}`"
        )

    # -------------------------------------------------------------------------------- #
    # Writers - Polars                                                                 #
    # -------------------------------------------------------------------------------- #

    def _validate_mode_polars(self, mode, df):
        if mode is not None:
            raise ValueError(
                f"Mode '{mode}' is not supported for Polars DataFrame. Set to `None`"
            )

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

    def _write_polars(self, df, mode, full_refresh) -> None:
        raise NotImplementedError(
            f"`{self.dataframe_backend}` not supported for `{type(self)}`"
        )

    # ----------------------------------------------------------------------- #
    # Purge                                                                   #
    # ----------------------------------------------------------------------- #

    def exists(self):
        if self.dataframe_backend == DataFrameBackends.PYSPARK:
            from laktory import get_spark_session

            try:
                spark = get_spark_session()
                df = self.read(spark=spark, as_stream=False)
                df.limit(1).collect()
                return True
            except Exception:
                return False

        else:
            raise NotImplementedError()

    def _purge_checkpoint(self):
        if self.checkpoint_path:
            if os.path.exists(self.checkpoint_path):
                logger.info(
                    f"Deleting checkpoint at {self.checkpoint_path}",
                )
                shutil.rmtree(self.checkpoint_path)

            if self.dataframe_backend != DataFrameBackends.PYSPARK:
                return

            try:
                from pyspark.dbutils import DBUtils

                from laktory import get_spark_session

                spark = get_spark_session()
            except ModuleNotFoundError:
                return

            dbutils = DBUtils(spark)

            _path = self.checkpoint_path.as_posix()
            try:
                dbutils.fs.ls(
                    _path
                )  # TODO: Figure out why this does not work with databricks connect
                logger.info(
                    f"Deleting checkpoint at dbfs {_path}",
                )
                dbutils.fs.rm(_path, True)

            except Exception as e:
                if "java.io.FileNotFoundException" in str(e):
                    pass
                elif "databricks.sdk.errors.platform.ResourceDoesNotExist" in str(
                    type(e)
                ):
                    pass
                elif "databricks.sdk.errors.platform.InvalidParameterValue" in str(
                    type(e)
                ):
                    # TODO: Figure out why this is happening. It seems that the databricks SDK
                    #       modify the path before sending to REST API.
                    logger.warn(f"dbutils could not delete checkpoint {_path}: {e}")
                else:
                    raise e

    def purge(self):
        """
        Delete sink data and checkpoints
        """
        raise NotImplementedError()

    # ----------------------------------------------------------------------- #
    # Data Sources                                                            #
    # ----------------------------------------------------------------------- #

    def as_source(self, as_stream=None):
        raise NotImplementedError()

    def read(self, as_stream=None):
        """
        Read dataframe from sink.

        Parameters
        ----------
        as_stream:
            If `True`, dataframe read as stream.

        Returns
        -------
        AnyFrame
            DataFrame
        """
        return self.as_source(as_stream=as_stream).read()
