from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.typing import AnyFrame

logger = get_logger(__name__)

SUPPORTED_BACKENDS = [DataFrameBackends.PYSPARK]


class DataSinkMergeCDCOptions(BaseModel):
    """
    Options for merging a change data capture (CDC).

    They are also used to build the target using `apply_changes` method when
    using Databricks DLT.

    Examples
    --------
    ```py
    from laktory import models

    df = spark.createDataFrame(
        [
            {"id": 1, "value": 3.0},
            {"id": 2, "value": 2.3},
            {"id": 3, "value": 7.7},
        ]
    )

    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        mode="MERGE",
        merge_cdc_options={
            "scd_type": 1,
            "primary_keys": ["id"],
        },
    )
    # sink.write(df)
    ```

    References
    ----------
    * [Change Data Capture](https://www.laktory.ai/concepts/cdc/)
    * [How to Implement SCD 2 using Delta Table](https://iterationinsights.com/article/how-to-implement-slowly-changing-dimensions-scd-type-2-using-delta-table/)
    * [Change Data Capture with Databricks DLT](https://docs.databricks.com/en/delta-live-tables/python-ref.html#change-data-capture-with-python-in-delta-live-tables)
    """

    # apply_as_truncates: Union[str, None] = None
    delete_where: str = Field(
        None,
        description="Specifies when a CDC event should be treated as a DELETE rather than an upsert.",
    )
    end_at_column_name: str = Field(
        "__end_at",
        description="""
        When using SCD type 2, name of the column storing the end time (or sequencing index) during which a row is 
        active. This attribute is not used when using Databricks DLT which does not allow column rename.
        """,
    )
    exclude_columns: list[str] = Field(
        None, description="A subset of columns to exclude in the target table."
    )
    ignore_null_updates: bool = Field(
        False,
        description="""
        Allow ingesting updates containing a subset of the target columns. When a CDC event matches an existing row and
        ignore_null_updates is `True`, columns with a null will retain their existing values in the target. This also 
        applies to nested columns with a value of null. When ignore_null_updates is `False`, existing values will be 
        overwritten with null values.
        """,
    )
    include_columns: list[str] = Field(
        None,
        description="""
        A subset of columns to include in the target table. Use `include_columns` to specify the complete list of 
        columns to include.
        """,
    )
    order_by: str = Field(
        None,
        description="""
        The column name specifying the logical order of CDC events in the source data. Used to handle
        change events that arrive out of order.
        """,
    )
    primary_keys: list[str] = Field(
        None,
        description="""
        The column or combination of columns that uniquely identify a row in the source data. This is used to 
        identify which CDC events apply to specific records in the target table.
        """,
    )
    scd_type: Literal[1, 2] = Field(
        1, description="Whether to store records as SCD type 1 or SCD type 2."
    )
    start_at_column_name: str = Field(
        "__start_at",
        description="""
        When using SCD type 2, name of the column storing the start time (or sequencing index) during which 
        a row is active. This attribute is not used when using Databricks DLT which does not allow column rename.
        """,
    )
    # track_history_columns: Union[list[str], None] = None
    # track_history_except_columns: Union[list[str], None] = None
    _parent: Any = None
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

        if self.exclude_columns and self.order_by:
            if self.order_by in self.exclude_columns:
                raise ValueError(
                    f"`order_by` '{self.order_by}' can't be excluded as it will prevent proper merge of out-of-sequence records."
                )

        return self

    # ----------------------------------------------------------------------- #
    # Sink                                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def sink(self):
        return self._parent

    @property
    def target_name(self):
        from laktory.models.datasinks.tabledatasink import TableDataSink

        if self._parent and isinstance(self._parent, TableDataSink):
            return self._parent.full_name
        return None

    @property
    def target_path(self):
        from laktory.models.datasinks.filedatasink import FileDataSink

        if self._parent and isinstance(self._parent, FileDataSink):
            return self._parent.path
        return None

    @property
    def target_id(self):
        return self.target_name or self.target_path

    # ----------------------------------------------------------------------- #
    # CDC Columns                                                             #
    # ----------------------------------------------------------------------- #

    @property
    def index(self):
        return self.order_by

    @property
    def start_at(self):
        return self.start_at_column_name

    @property
    def end_at(self):
        return self.end_at_column_name

    @property
    def index_fist(self):
        return self.index + "_first"

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
        cols = []
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

    def _init_target(self, source):
        import pyspark.sql.types as T

        spark = source.sparkSession
        logger.info(f"Merge target not found. Creating empty table at {self.target_id}")
        schema = source.select(self.primary_keys + self.update_columns).schema
        if self.scd_type == 2:
            schema.add(T.StructField(self.hash_cols, T.StringType(), True))
            schema.add(T.StructField(self.start_at, self.index_type, True))
            schema.add(T.StructField(self.end_at, self.index_type, True))

        df = spark.createDataFrame(data=[], schema=schema)

        writer = df.write.format("delta").mode("OVERWRITE")
        # if self.sink.cluster_by:
        #     writer = writer.clusterBy(*self.sink.cluster_by)
        if self.target_path:
            writer.save(self.target_path)
        else:
            writer.saveAsTable(self.target_name)

    def _execute(self, source: AnyFrame):
        import pyspark.sql.functions as F
        from delta.tables import DeltaTable
        from pyspark.sql import Window

        spark = source.sparkSession

        logger.info(
            f"Executing merge on {self.target_id} with primary keys {self.primary_keys} and scd type {self.scd_type}"
        )

        if self.delete_where:
            logger.info(f"with delete on {self.delete_where}")

        # Add internal columns
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
                logger.info(
                    f"Dropping duplicates using {self.primary_keys} and '{self.order_by}' as sequencing index"
                )
                source = source.filter(F.col("_row_number") == 1)
            elif self.scd_type == 2:
                # Assign previous index to ends_at
                w2 = Window.partitionBy(*self.primary_keys)
                source = source.withColumn(self.end_at, F.lag(self.index, 1).over(w))
                source = source.withColumn(self.index_fist, F.min(self.index).over(w2))
            source = source.drop("_row_number")
        else:
            logger.info(f"Dropping duplicates using {self.primary_keys}")
            source = source.drop_duplicates(subset=self.primary_keys)

        # Read target
        if self.target_path:
            table_target = DeltaTable.forPath(spark, self.target_path)
        else:
            table_target = DeltaTable.forName(spark, self.target_name)

        if self.scd_type == 1:
            if self.delete_where:
                delete_condition = F.coalesce(
                    F.expr(self.source_delete_where), F.lit(False)
                )
                not_delete_condition = ~delete_condition

            # Define merge
            conditions = [f"source.{c} = target.{c}" for c in self.primary_keys]
            merge = table_target.alias("target").merge(
                source.alias("source"),
                condition=" AND ".join(conditions),
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
                condition = not_delete_condition
            if self.order_by:
                _condition = F.expr(f"source.{self.order_by} > target.{self.order_by}")
                if condition is None:
                    condition = _condition
                else:
                    condition = condition & _condition

            merge = merge.whenMatchedUpdate(set=_set, condition=condition)

            # Insert
            condition = None
            if self.delete_where:
                condition = not_delete_condition
            merge = merge.whenNotMatchedInsert(
                values={f"target.{c}": f"source.{c}" for c in self.write_columns},
                condition=condition,
            )

            # Delete
            if self.delete_where:
                merge = merge.whenMatchedDelete(condition=delete_condition)

            logger.info("Executing merge...")
            merge.execute()

        elif self.scd_type == 2:
            if self.delete_where:
                delete_condition = F.coalesce(F.expr(self.delete_where), F.lit(False))
                not_delete_condition = ~delete_condition

            # Only select rows that have been updated
            if self.target_path:
                target = spark.read.format("delta").load(self.target_path)
            else:
                target = spark.read.table(self.target_name)

            _source = source
            _target = target
            _on = [self.hash_cols] + self.primary_keys
            if self.delete_where:
                _source = source.withColumn("__to_delete", delete_condition)
                _target = target.withColumn("__to_delete", F.lit(False))
                _on += ["__to_delete"]

            upsert_or_delete = _source.join(
                other=_target,
                on=_on,
                how="leftanti",
            )

            # Merge
            conditions = []
            condition = F.expr(f"target.{self.end_at} IS NULL")
            for c in self.primary_keys:
                condition = condition & F.expr(f"source.{c} = target.{c}")
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

            logger.info("Executing merge...")
            merge.execute()

            # Append rows
            upsert = upsert_or_delete
            if self.delete_where:
                upsert = upsert.filter(not_delete_condition)
            writer = (
                upsert.select(self.write_columns).write.mode("APPEND").format("DELTA")
            )
            logger.info("Appending new rows...")
            if self.target_path:
                writer.save(self.target_path)
            else:
                writer.saveAsTable(self.target_name)

        else:
            raise ValueError(f"SCD Type {self.scd_type} is not supported.")

    def execute(self, source: AnyFrame):
        """
        Merge source into target delta from sink

        Parameters
        ----------
        source:
            Source DataFrame to merge into target (sink).
        """

        dataframe_backend = DataFrameBackends.from_nw_implementation(
            source.implementation
        )
        if dataframe_backend not in SUPPORTED_BACKENDS:
            raise NotImplementedError(
                f"DataFrame provided is of {dataframe_backend} backend, which is not currently implemented for merge operations."
            )

        source = source.to_native()

        from delta.tables import DeltaTable

        self._source_schema = source.schema
        spark = source.sparkSession

        if self.target_path:
            if not DeltaTable.isDeltaTable(spark, self.target_path):
                self._init_target(source)
        else:
            try:
                spark.catalog.getTable(self.target_name)
            except Exception:
                self._init_target(source)

        if source.isStreaming:
            if self.sink is None:
                raise ValueError("Sink value required to fetch checkpoint location.")

            if self.sink and self.sink.checkpoint_path is None:
                raise ValueError(
                    f"Checkpoint location not specified for sink '{self.sink}'"
                )

            query = (
                source.writeStream.foreachBatch(
                    lambda batch_df, batch_id: self._execute(source=batch_df)
                )
                .trigger(availableNow=True)
                .options(
                    checkpointLocation=self.sink.checkpoint_path,
                )
                .start()
            )
            query.awaitTermination()

        else:
            self._execute(source=source)
