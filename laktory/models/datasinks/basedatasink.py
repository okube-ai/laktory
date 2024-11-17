import os
import hashlib
import uuid
import shutil
from pathlib import Path
from typing import Union
from typing import Any
from typing import Literal
from pydantic import Field
from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.spark import is_spark_dataframe
from laktory.spark import SparkDataFrame
from laktory.polars import is_polars_dataframe
from laktory.polars import PolarsLazyFrame
from laktory.types import AnyDataFrame

logger = get_logger(__name__)


class DataSinkMergeCDCOptions(BaseModel):
    """
    Options for merging a change data capture (CDC).

    They are also used to build the target using `apply_changes` method when
    using Databricks DLT.

    Attributes
    ----------
    delete_where:
        Specifies when a CDC event should be treated as a DELETE rather than
        an upsert.
    apply_as_truncates:
        Specifies when a CDC event should be treated as a full table TRUNCATE.
        Because this clause triggers a full truncate of the target table, it
        should be used only for specific use cases requiring this
        functionality.
    include_columns:
        A subset of columns to include in the target table. Use
        `include_columns` to specify the complete list of columns to include.
    exclude_columns:
        A subset of columns to exclude in the target table.
    ignore_null_updates:
        Allow ingesting updates containing a subset of the target columns.
        When a CDC event matches an existing row and ignore_null_updates is
        `True`, columns with a null will retain their existing values in the
        target. This also applies to nested columns with a value of null. When
        ignore_null_updates is `False`, existing values will be overwritten
        with null values.
    primary_keys:
        The column or combination of columns that uniquely identify a row in
        the source data. This is used to identify which CDC events apply to
        specific records in the target table.
    scd_type:
        Whether to store records as SCD type 1 or SCD type 2.
    order_by:
        The column name specifying the logical order of CDC events in the
        source data. Used to handle change events that arrive out of order.
    track_history_columns:
        A subset of output columns to be tracked for history in the target table.
    track_history_except_columns:
        A subset of output columns to be excluded from tracking.

    References
    ----------
    https://docs.databricks.com/en/delta-live-tables/python-ref.html#change-data-capture-with-python-in-delta-live-tables
    """

    # apply_as_truncates: Union[str, None] = None
    include_columns: list[str] = None
    exclude_columns: list[str] = None
    delete_where: str = None
    ignore_null_updates: bool = False
    order_by: str = None
    primary_keys: list[str]
    scd_type: Literal[1, 2] = None
    # track_history_columns: Union[list[str], None] = None
    # track_history_except_columns: Union[list[str], None] = None

    def execute(self, target_path, source: SparkDataFrame, node=None):

        if source.isStreaming:

            if node._checkpoint_location is None:
                raise ValueError(
                    f"Expectations Checkpoint not specified for node '{node.name}'"
                )

            query = (
                source.writeStream.foreachBatch(
                    lambda batch_df, batch_id: self._execute(
                        target_path=target_path,
                        source=batch_df,
                    )
                )
                .trigger(availableNow=True)
                .options(
                    checkpointLocation=node._checkpoint_location,
                )
                .start()
            )
            query.awaitTermination()

        else:
            self._execute(target_path=target_path, source=source)

    def _execute(self, target_path, source: SparkDataFrame):

        from delta.tables import DeltaTable
        from pyspark.sql import Window
        import pyspark.sql.functions as F

        spark = source.sparkSession

        logger.info(f"Executing merge on {target_path} with primary keys {self.primary_keys}")

        # Select columns
        if self.include_columns:
            columns = [c for c in self.include_columns]
        else:
            columns = [c for c in source.columns if c not in self.primary_keys]
            if self.exclude_columns:
                columns = [c for c in columns if c not in self.exclude_columns]

        # Drop Duplicates
        if self.order_by:
            w = Window.partitionBy(*self.primary_keys).orderBy(F.desc(self.order_by))
            source = (
                source
                .withColumn("_row_number", F.row_number().over(w))
                .filter(F.col("_row_number") == 1)
                .drop("_row_number")
            )

        table_target = DeltaTable.forPath(spark, target_path)

        # Define merge
        merge = (
            table_target.alias("target")
            .merge(
                source.alias("source"),
                condition=" AND ".join([f"source.{c} = target.{c}" for c in self.primary_keys]),
            )
        )

        # Update
        _set = {f"target.{c}": f"source.{c}" for c in columns}
        if self.ignore_null_updates:
            _set = {
                f"target.{c}": F.coalesce(F.col(f"source.{c}"), F.col(f"target.{c}")).alias(c)
                for c in columns
            }
        if not self.delete_where:
            merge = merge.whenMatchedUpdate(set=_set)
        else:
            merge = merge.whenMatchedUpdate(set=_set, condition=~F.expr(self.delete_where))

        # Insert
        merge = merge.whenNotMatchedInsert(
            values={f"target.{c}": f"source.{c}" for c in self.primary_keys + columns}
        )

        # Delete
        if self.delete_where:
            merge = merge.whenMatchedDelete(condition=self.delete_where)

        merge.execute()


class BaseDataSink(BaseModel):
    """
    Base class for building data sink

    Attributes
    ----------
    is_primary:
        A primary sink will be used to read data for downstream nodes when
        moving from stream to batch. Don't apply for quarantine sinks.
    is_quarantine:
        Sink used to store quarantined results from node expectations.
    merge_cdc_options:
        Merge options to handle input DataFrames that are Change Data Capture
        (CDC). Only used when `merge` mode is selected.
    mode:
        Write mode.
        - overwrite: Overwrite existing data
        - append: Append contents of the dataframe to existing data
        - error: Throw and exception if data already exists
        - ignore: Silently ignore this operation if data already exists
        - complete: Overwrite for streaming dataframes
        - merge: Append, update and optionally delete records. Requires
        cdc specification.
    write_options:
        Other options passed to `spark.write.options`
    """

    is_quarantine: bool = False
    is_primary: bool = True
    checkpoint_location: str = None
    merge_cdc_options: DataSinkMergeCDCOptions = None  # TODO: Review parameter name
    mode: Union[
        Literal["OVERWRITE", "APPEND", "IGNORE", "ERROR", "COMPLETE", "UPDATE", "MERGE"], None
    ] = None
    write_options: dict[str, str] = {}
    _parent: "PipelineNode" = None

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        return str(self)

    @property
    def _uuid(self) -> str:
        hash_object = hashlib.sha1(self._id.encode())
        hash_digest = hash_object.hexdigest()
        return str(uuid.UUID(hash_digest[:32]))

    @property
    def _checkpoint_location(self) -> Path:

        if self.checkpoint_location:
            return Path(self.checkpoint_location)

        if self._parent and self._parent._root_path:
            for i, s in enumerate(self._parent.all_sinks):
                if s == self:
                    return (
                        self._parent._root_path / "checkpoints" / f"sink-{self._uuid}"
                    )

        return None

    # ----------------------------------------------------------------------- #
    # Writers                                                                 #
    # ----------------------------------------------------------------------- #

    def write(self, df: AnyDataFrame, mode=None) -> None:
        """
        Write dataframe into sink.

        Parameters
        ----------
        df:
            Input dataframe
        mode:
            Write mode overwrite of the sink default mode.

        Returns
        -------
        """
        if mode is None:
            mode = self.mode
        if is_spark_dataframe(df):
            self._write_spark(df, mode=mode)
        elif is_polars_dataframe(df):
            self._write_polars(df, mode=mode)
        else:
            raise ValueError(f"DataFrame type '{type(df)}' not supported")

        logger.info("Write completed.")

    def _write_spark(self, df: SparkDataFrame, mode=mode) -> None:
        raise NotImplementedError("Not implemented for Spark dataframe")

    def _write_polars(self, df: PolarsLazyFrame, mode=mode) -> None:
        raise NotImplementedError("Not implemented for Polars dataframe")

    # ----------------------------------------------------------------------- #
    # Purge                                                                   #
    # ----------------------------------------------------------------------- #

    def _purge_checkpoint(self, spark=None):
        if self._checkpoint_location:
            if os.path.exists(self._checkpoint_location):
                logger.info(
                    f"Deleting checkpoint at {self._checkpoint_location}",
                )
                shutil.rmtree(self._checkpoint_location)

            if spark is None:
                return

            try:
                from pyspark.dbutils import DBUtils
            except ModuleNotFoundError:
                return

            dbutils = DBUtils(spark)

            _path = self._checkpoint_location.as_posix()
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
    # Sources                                                                 #
    # ----------------------------------------------------------------------- #

    def as_source(self, as_stream=None):
        raise NotImplementedError()

    def read(self, spark=None, as_stream=None):
        """
        Read dataframe from sink.

        Parameters
        ----------
        spark:
            Spark Session
        as_stream:
            If `True`, dataframe read as stream.

        Returns
        -------
        """
        return self.as_source(as_stream=as_stream).read(spark=spark)
