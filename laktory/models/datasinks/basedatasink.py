import hashlib
import uuid
from pathlib import Path
from typing import Any
from typing import Literal

import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.datasinks.mergecdcoptions import DataSinkMergeCDCOptions
from laktory.models.pipeline.pipelinechild import PipelineChild
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
SUPPORTED_MODES = list(set(SPARK_MODES + SPARK_STREAMING_MODES + POLARS_DELTA_MODES))


class WriterMethod(BaseModel):
    """
    Writer Method

    Calling DataFrame backend writer method. Implementation specific to a given backend.
    """

    name: str = Field(..., description="Method name")
    args: list[Any] = Field([], description="Method arguments")
    kwargs: dict[str, Any] = Field({}, description="Method keyword arguments")

    @property
    def as_string(self) -> str:
        s = f"{self.name}("
        s += ",".join([str(v) for v in self.args])
        s += ",".join([f"{k}={v}" for k, v in self.kwargs.items()])
        s += ")"
        return s


class BaseDataSink(BaseModel, PipelineChild):
    """Base class for data sinks"""

    checkpoint_path: str = Field(
        None,
        description="Path to which the checkpoint file for which a streaming dataframe should be written.",
    )
    type: Literal["FILE", "HIVE_METASTORE", "UNITY_CATALOG"] = Field(
        ..., description="Name of the data sink type"
    )
    merge_cdc_options: DataSinkMergeCDCOptions = Field(
        None,
        description="Merge options to handle input DataFrames that are Change Data Capture (CDC). Only used when `MERGE` mode is selected.",
    )  # TODO: Review parameter name
    mode: Literal.__getitem__(SUPPORTED_MODES) = Field(
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
    writer_methods: list[WriterMethod] = Field(
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
        return str(self)

    @property
    def _uuid(self) -> str:
        hash_object = hashlib.sha1(self._id.encode())
        hash_digest = hash_object.hexdigest()
        return str(uuid.UUID(hash_digest[:32]))

    @property
    def _checkpoint_path(self) -> Path | None:
        if self.checkpoint_path:
            return Path(self.checkpoint_path)

        node = self.parent_pipeline_node

        if node and node._root_path:
            for i, s in enumerate(node.all_sinks):
                if s == self:
                    return node._root_path / "checkpoints" / f"sink-{self._uuid}"
        return None

    # ----------------------------------------------------------------------- #
    # CDC                                                                     #
    # ----------------------------------------------------------------------- #

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
        if self.df_backend == DataFrameBackends.POLARS:
            self._validate_mode_polars(mode, df)
        elif self.df_backend == DataFrameBackends.PYSPARK:
            self._validate_mode_spark(mode, df)

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

    def _validate_mode_polars(self, mode, df):
        if mode is not None:
            raise ValueError(
                f"Mode '{mode}' is not supported for Polars DataFrame. Set to `None`"
            )

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

        # TODO: Add support for view definition

        if mode is None:
            mode = self.mode

        if not isinstance(df, (nw.DataFrame, nw.LazyFrame)):
            df = nw.from_native(df)

        dataframe_backend = DataFrameBackends.from_nw_implementation(df.implementation)
        if dataframe_backend not in SUPPORTED_BACKENDS:
            raise ValueError(
                f"DataFrame provided is of {dataframe_backend} backend, which is not supported."
            )

        if self.dataframe_backend and self.dataframe_backend != dataframe_backend:
            raise ValueError(
                f"DataFrame provided is {dataframe_backend} and source has been configure with {self.dataframe_backend} backend."
            )
        self.dataframe_backend = dataframe_backend

        self._validate_mode(mode, df)
        self._validate_format()

        if mode.lower() == "merge":
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

    #
    # def _write_spark_view(self, view_definition: str, spark) -> None:
    #     raise NotImplementedError(
    #         f"View creation with spark is not implemented for type '{type(self)}'"
    #     )
    #

    def _write_spark(self, df, mode, full_refresh) -> None:
        raise NotImplementedError(
            f"`{self.df_backend}` not supported for `{type(self)}`"
        )

    def _write_polars(self, df, mode, full_refresh) -> None:
        raise NotImplementedError(
            f"`{self.df_backend}` not supported for `{type(self)}`"
        )
