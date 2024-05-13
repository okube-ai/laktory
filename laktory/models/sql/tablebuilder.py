from pydantic import model_validator
from typing import Any
from typing import Literal
from typing import Union

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.datasources import FileDataSource
from laktory.models.datasources import TableDataSource
from laktory.models.datasources import MemoryDataSource
from laktory.models.sql.column import Column
from laktory.models.spark.sparkchain import SparkChain
from laktory.models.spark.sparkchainnode import SparkChainNode
from laktory.spark import SparkDataFrame

logger = get_logger(__name__)


class TableBuilder(BaseModel):
    """
    Mechanisms for building a table from a source in the context of a data
    pipeline.

    Attributes
    ----------
    add_laktory_columns:
        Add laktory default columns like layer-specific timestamps
    as_dlt_view:
        Create (DLT) view instead of table
    drop_duplicates:
        If `True`:
            - drop duplicated rows using `primary_key` if defined or all
              columns if not defined.
        If list of strings:
            - drop duplicated rows using `drop_duplicates` as the subset.
    drop_source_columns:
        If `True`, drop columns from the source after read and only keep
        columns defined in the table and/or resulting from the joins.
    layer:
        Layer in the medallion architecture
    pipeline_name:
        Name of the pipeline in which the table will be built
    source:
        Definition of the data source
    spark_chain:
        Spark chain defining the data transformations applied to the data
        source
    template:
        Key indicating which notebook to use for building the table in the
        context of a data pipeline.
    """

    add_laktory_columns: bool = True
    as_dlt_view: bool = False
    drop_duplicates: Union[bool, None] = None
    drop_source_columns: Union[bool, None] = None
    source: Union[FileDataSource, TableDataSource, MemoryDataSource, None] = None
    layer: Literal["BRONZE", "SILVER", "GOLD"] = None
    pipeline_name: Union[str, None] = None
    template: Union[str, bool, None] = None
    spark_chain: Union[SparkChain, None] = None
    _table: Any = None

    @model_validator(mode="after")
    def default_values(self) -> Any:
        """
        Sets default options like `drop_source_columns`, `drop_duplicates`,
        `template`, etc. based on `layer` value.
        """
        # Default values
        if self.layer == "BRONZE":
            if self.drop_source_columns is None:
                self.drop_source_columns = False
            if self.drop_duplicates is not None:
                self.drop_duplicates = False

        if self.layer == "SILVER":
            if self.drop_source_columns is None:
                self.drop_source_columns = True
            if self.drop_duplicates is not None and self.primary_key:
                self.drop_duplicates = True
        #
        # if self.layer == "SILVER_STAR":
        #     if self.drop_source_columns is None:
        #         self.drop_source_columns = False
        #     if self.drop_duplicates is not None:
        #         self.drop_duplicates = False
        #
        # if self.layer == "GOLD":
        #     if self.drop_source_columns is None:
        #         self.drop_source_columns = False
        #     if self.drop_duplicates is not None:
        #         self.drop_duplicates = False
        #
        if self.template is None:
            self.template = self.layer

        return self

    @property
    def is_from_cdc(self) -> bool:
        """If `True` CDC source is used to build the table"""
        if self.source is None:
            return False
        else:
            return self.source.is_cdc

    @property
    def columns(self) -> list[Column]:
        """List of columns"""
        if self._table is None:
            return []
        return self._table.columns

    @property
    def timestamp_key(self) -> str:
        """Table timestamp key"""
        if self._table is None:
            return None
        return self._table.timestamp_key

    @property
    def primary_key(self) -> str:
        """Table primary key"""
        if self._table is None:
            return None
        return self._table.primary_key

    @property
    def layer_spark_chain(self):
        nodes = []

        if self.layer == "BRONZE":
            if self.add_laktory_columns:
                nodes += [
                    SparkChainNode(
                        column={
                            "name": "_bronze_at",
                            "type": "timestamp",
                        },
                        spark_func_name="current_timestamp",
                    ),
                ]

        elif self.layer == "SILVER":
            if self.timestamp_key:
                nodes += [
                    SparkChainNode(
                        column={
                            "name": "_tstamp",
                            "type": "timestamp",
                        },
                        sql_expression=self.timestamp_key,
                    )
                ]

            if self.add_laktory_columns:
                nodes += [
                    SparkChainNode(
                        column={
                            "name": "_silver_at",
                            "type": "timestamp",
                        },
                        spark_func_name="current_timestamp",
                    )
                ]

        elif self.layer == "GOLD":
            if self.add_laktory_columns:
                nodes += [
                    SparkChainNode(
                        column={
                            "name": "_gold_at",
                            "type": "timestamp",
                        },
                        spark_func_name="current_timestamp",
                    )
                ]

        if self.drop_duplicates:
            subset = None
            if isinstance(self.drop_duplicates, list):
                subset = self.drop_duplicates
            elif self.primary_key:
                subset = [self.primary_key]

            nodes += [
                SparkChainNode(
                    spark_func_name="dropDuplicates", spark_func_args=[subset]
                )
            ]

        if self.drop_source_columns:
            nodes += [
                SparkChainNode(
                    spark_func_name="drop",
                    spark_func_args=[
                        c
                        for c in self.spark_chain.columns[0]
                        if c not in ["_bronze_at", "_silver_at", "_gold_at"]
                    ],
                )
            ]

        if len(nodes) == 0:
            return None

        return SparkChain(nodes=nodes)

    def read_source(self, spark) -> SparkDataFrame:
        """
        Read data source specified in `self.source`

        Parameters
        ----------
        spark: SparkSession
            Spark session

        Returns
        -------
        :
            Output dataframe
        """
        return self.source.read(spark)

    def process(self, df, udfs=None, spark=None) -> SparkDataFrame:
        """
        Build table by reading source and applying Spark Chain.

        Parameters
        ----------
        df:
            Input DataFrame
        udfs:
            User-defined functions
        spark: SparkSession
            Spark sessions.

        Returns
        -------
        :
            output Spark DataFrame
        """
        logger.info(f"Applying {self.layer} transformations")

        if self.spark_chain is None:
            return df

        df = self.spark_chain.execute(df, udfs=udfs, spark=spark)
        if self.layer_spark_chain:
            df = self.layer_spark_chain.execute(df, udfs=udfs, spark=spark)

        return df

    @property
    def apply_changes_kwargs(self) -> dict[str, str]:
        """Keyword arguments for dlt.apply_changes function"""
        cdc = self.source.cdc
        return {
            "apply_as_deletes": cdc.apply_as_deletes,
            "apply_as_truncates": cdc.apply_as_truncates,
            "column_list": cdc.columns,
            "except_column_list": cdc.except_columns,
            "ignore_null_updates": cdc.ignore_null_updates,
            "keys": cdc.primary_keys,
            "sequence_by": cdc.sequence_by,
            "source": self.source.name,
            "stored_as_scd_type": cdc.scd_type,
            "target": self._table.name,
            "track_history_column_list": cdc.track_history_columns,
            "track_history_except_column_list": cdc.track_history_except_columns,
        }
