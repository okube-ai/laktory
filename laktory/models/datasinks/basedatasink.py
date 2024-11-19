import os
import hashlib
import uuid
import shutil
from pathlib import Path
from typing import Union
from typing import Any
from typing import Literal
from pydantic import model_validator
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
    end_at_column_name:
        When using SCD type 2, name of the column storing the end time (or
        sequencing index) during which a row is active. This attribute is not
        used when using Databricks DLT which does not allow column rename.
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
    order_by:
        The column name specifying the logical order of CDC events in the
        source data. Used to handle change events that arrive out of order.
    primary_keys:
        The column or combination of columns that uniquely identify a row in
        the source data. This is used to identify which CDC events apply to
        specific records in the target table.
    scd_type:
        Whether to store records as SCD type 1 or SCD type 2.
    start_at_column_name:
        When using SCD type 2, name of the column storing the start time (or
        sequencing index) during which a row is active. This attribute is not
        used when using Databricks DLT which does not allow column rename.

    References
    ----------
    https://docs.databricks.com/en/delta-live-tables/python-ref.html#change-data-capture-with-python-in-delta-live-tables
    """

    # apply_as_truncates: Union[str, None] = None
    delete_where: str = None
    end_at_column_name: str = "__end_at"
    exclude_columns: list[str] = None
    ignore_null_updates: bool = False
    include_columns: list[str] = None
    order_by: str = None
    primary_keys: list[str]
    scd_type: Literal[1, 2] = 1
    start_at_column_name: str = "__start_at"
    # track_history_columns: Union[list[str], None] = None
    # track_history_except_columns: Union[list[str], None] = None
    _source_schema: Any = None
    _source_columns: list[str] = None

    @model_validator(mode="after")
    def validate_scd_type(self) -> Any:
        if self.scd_type == 2:
            if self.order_by is None:
                raise ValueError(
                    "SCD Type 2 merge requires specific of `order_by` attribute."
                )

        return self

    @model_validator(mode="after")
    def selected_columns(self) -> Any:
        if self.include_columns and self.exclude_columns:
            raise ValueError(
                "`include_columns` and `exclude_columns` attributes are mutually exclusive."
            )

        return self

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    @staticmethod
    def _add_alias(expr, prefix="source"):

        operators = ["=", ">", "<", "!", "*", "+", "-", "/", ","]

        new_expr = expr
        for o in operators:
            new_expr = new_expr.replace(o, " ")

        candidates = [c for c in new_expr.split(" ") if c != ""]

        # Define other exclusions for SQL keywords or constants
        exclusions = [
            "TRUE",
            "FALSE",
            "CASE",
            "WHEN",
            "THEN",
            "SELECT",
            "FROM",
            "WHERE",
            "AND",
            "OR",
            "AS",
            "END",
            "ELSE",
            "NOT",
            "NULL",
        ]

        # Filter out identifiers that are constants or keywords
        column_names = []
        for c in candidates:
            if c.upper() in exclusions:
                continue
            if c.startswith("'"):
                continue
            try:
                float(c)
                continue
            except ValueError:
                pass

            column_names += [c]

        new_expr = expr
        for c in column_names:
            if f"{prefix}." not in c:
                new_expr = new_expr.replace(c, f"{prefix}.{c}")

        return new_expr

    @property
    def start_at(self):
        return self.start_at_column_name

    @property
    def end_at(self):
        return self.end_at_column_name

    @property
    def index(self):
        return self.order_by

    @property
    def index_fist(self):
        return self.index + "_first"

    @property
    def hash_keys(self):
        return "__hash_keys"

    @property
    def hash_cols(self):
        return "__hash_cols"

    @property
    def source_columns(self):
        return [f.name for f in self._source_schema]

    @property
    def update_columns(self):
        if self.include_columns:
            columns = [c for c in self.include_columns]
        else:
            columns = [c for c in self.source_columns if c not in self.primary_keys]
            if self.exclude_columns:
                columns = [c for c in columns if c not in self.exclude_columns]
        return columns

    @property
    def extra_columns(self):
        cols = [self.hash_keys]
        if self.scd_type == 2:
            cols += [self.start_at, self.end_at, self.hash_cols]
        return cols

    @property
    def write_columns(self):
        return self.primary_keys + self.update_columns + self.extra_columns

    @property
    def index_type(self):
        return [
            field.dataType
            for field in self._source_schema.fields
            if field.name == self.index
        ][0]

    @property
    def source_delete_where(self):
        return self._add_alias(self.delete_where)

    def _init_target(self, source, target_path):

        import pyspark.sql.types as T

        spark = source.sparkSession
        logger.info(f"Merge target not found. Creating empty table at {target_path}")
        schema = source.select(self.primary_keys + self.update_columns).schema
        schema.add(T.StructField(self.hash_keys, T.StringType(), True))
        if self.scd_type == 2:
            schema.add(T.StructField(self.hash_cols, T.StringType(), True))
            schema.add(T.StructField(self.start_at, self.index_type, True))
            schema.add(T.StructField(self.end_at, self.index_type, True))

        df = spark.createDataFrame(data=[], schema=schema)
        df.write.format("DELTA").mode("OVERWRITE").save(target_path)

    def _execute(self, target_path, source: SparkDataFrame):

        from delta.tables import DeltaTable
        from pyspark.sql import Window
        import pyspark.sql.functions as F

        spark = source.sparkSession

        logger.info(
            f"Executing merge on {target_path} with primary keys {self.primary_keys} and scd type {self.scd_type}"
        )

        # Add internal columns
        source = source.withColumn(
            self.hash_keys, F.lit(F.sha2(F.concat_ws("~", *self.primary_keys), 256))
        )
        if self.scd_type == 2:
            source = (
                source.withColumn(self.start_at, F.col(self.index))
                .withColumn(self.end_at, F.lit(None).cast(self.index_type))
                .withColumn(
                    self.hash_cols,
                    F.lit(F.sha2(F.concat_ws("~", *self.update_columns), 256)),
                )
            )

        # Process History
        if self.index:
            w = Window.partitionBy(*self.primary_keys).orderBy(F.desc(self.index))
            source = source.withColumn("_row_number", F.row_number().over(w))
            if self.scd_type == 1:
                # Drop Duplicates
                source = source.filter(F.col("_row_number") == 1)
            elif self.scd_type == 2:
                # Assign previous index to ends_at
                w2 = Window.partitionBy(*self.primary_keys)
                source = source.withColumn(self.end_at, F.lag(self.index, 1).over(w))
                source = source.withColumn(self.index_fist, F.min(self.index).over(w2))
            source = source.drop("_row_number")

        # Read target
        table_target = DeltaTable.forPath(spark, target_path)

        if self.scd_type == 1:

            # Define merge
            merge = table_target.alias("target").merge(
                source.alias("source"),
                condition=f"source.{self.hash_keys} = target.{self.hash_keys}",
            )

            # Update
            _set = {f"target.{c}": f"source.{c}" for c in self.update_columns}
            if self.ignore_null_updates:
                _set = {
                    f"target.{c}": F.coalesce(
                        F.col(f"source.{c}"), F.col(f"target.{c}")
                    ).alias(c)
                    for c in self.update_columns
                }

            condition = None
            if self.delete_where:
                condition = ~F.expr(self.source_delete_where)
            merge = merge.whenMatchedUpdate(set=_set, condition=condition)

            # Insert
            merge = merge.whenNotMatchedInsert(
                values={f"target.{c}": f"source.{c}" for c in self.write_columns}
            )

            # Delete
            if self.delete_where:
                merge = merge.whenMatchedDelete(
                    condition=F.expr(self.source_delete_where)
                )

            merge.execute()

        elif self.scd_type == 2:

            # Only select rows that have been updated
            target = spark.read.format("DELTA").load(target_path)
            upsert_or_delete = source.withColumn(
                "__to_delete", F.expr(self.delete_where)
            ).join(
                other=target.withColumn("__to_delete", F.lit(False)),
                on=[self.hash_cols, self.hash_keys, "__to_delete"],
                how="leftanti",
            )

            # Merge
            condition = F.expr(f"source.{self.hash_keys} = target.{self.hash_keys}")
            condition = condition & F.expr(f"target.{self.end_at} IS NULL")
            merge = table_target.alias("target").merge(
                upsert_or_delete.filter(F.col(self.end_at).isNull()).alias("source"),
                condition=condition,
            )

            # Expire the current record
            _set = {f"target.{self.end_at}": f"source.{self.index_fist}"}
            merge = merge.whenMatchedUpdate(set=_set)
            #
            # # TODO: Review if required
            # if not self.delete_where:
            #     _set = {f"target.{self.end_at}": f"source.{self.order_by}"}
            #     merge = merge.whenMatchedUpdate(set=_set)
            # else:
            #     where = F.expr(self.source_delete_where)
            #     # deleting
            #     # _set = {f"target.{self.end_at}": "NULL"}
            #     _set = {f"target.{self.end_at}": f"source.{self.order_by}"}
            #     merge = merge.whenMatchedUpdate(set=_set, condition=where)
            #
            #     # updating
            #     _set = {f"target.{self.end_at}": f"source.{self.order_by}"}
            #     merge = merge.whenMatchedUpdate(set=_set, condition=~where)

            merge.execute()

            # Append rows
            upsert = upsert_or_delete
            if self.delete_where:
                upsert = upsert.filter(~F.expr(self.delete_where))
            (
                upsert.select(self.write_columns)
                .write.mode("APPEND")
                .format("DELTA")
                .save(target_path)
            )

        else:
            raise ValueError(f"SCD Type {self.scd_type} is not supported.")

    def execute(self, target_path, source: SparkDataFrame, sink=None):

        from delta.tables import DeltaTable

        self._source_schema = source.schema
        spark = source.sparkSession

        if not DeltaTable.isDeltaTable(spark, target_path):
            self._init_target(source, target_path)

        if source.isStreaming:

            if sink is None:
                raise ValueError(
                    f"Sink value required to fetch checkpoint location."
                )

            if sink and sink._checkpoint_location is None:
                raise ValueError(
                    f"Checkpoint location not specified for sink '{sink}'"
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
                    checkpointLocation=sink._checkpoint_location,
                )
                .start()
            )
            query.awaitTermination()

        else:
            self._execute(target_path=target_path, source=source)


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
        Literal[
            "OVERWRITE", "APPEND", "IGNORE", "ERROR", "COMPLETE", "UPDATE", "MERGE"
        ],
        None,
    ] = None
    write_options: dict[str, str] = {}
    _parent: "PipelineNode" = None

    @model_validator(mode="after")
    def merge_has_options(self) -> Any:
        if self.mode == "MERGE" and self.merge_cdc_options is None:
            raise ValueError(
                "If 'MERGE' `mode` is selected, `merge_cdc_options` must be specified."
            )

        return self

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
