import uuid
from typing import Any
from typing import Literal
from typing import Union
from typing import Callable
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.datasources import DataSourcesUnion
from laktory.models.datasinks import DataSinksUnion
from laktory.models.pipelinenodeexpectation import PipelineNodeExpectation
from laktory.models.spark.sparkchain import SparkChain
from laktory.models.spark.sparkchainnode import SparkChainNode
from laktory.spark import SparkSession
from laktory.types import AnyDataFrame
from laktory._logger import get_logger

logger = get_logger(__name__)


class PipelineNode(BaseModel):
    """
    Pipeline base component generating a DataFrame from a data source and a
    SparkChain transformation. Optional output to a data sink.

    Attributes
    ----------
    add_layer_columns:
        If `True` and `layer` not `None` layer-specific columns like timestamps
        are added to the resulting DataFrame.
    chain:
        Spark or Polars chain defining the data transformations applied to the
        data source
    drop_duplicates:
        If `True`:
            - drop duplicated rows using `primary_key` if defined or all
              columns if not defined.
        If list of strings:
            - drop duplicated rows using `drop_duplicates` as the subset.
    drop_source_columns:
        If `True`, drop columns from the source after read and only keep
        columns resulting from executing the SparkChain.
    expectations:
        List of expectations for the DataFrame. Can be used as warnings, drop
        invalid records or fail a pipeline.
    id:
        ID given to the node. Required to reference a node in a data source.
    layer:
        Layer in the medallion architecture
    primary_key:
        Name of the column storing a unique identifier for each row. It is used
        by the node to drop duplicated rows.
    source:
        Definition of the data source
    sink:
        Definition of the data sink
    timestamp_key:
        Name of the column storing a timestamp associated with each row. It is
        used as the default column by the builder when creating watermarks.


    Examples
    --------
    ```py
    # TODO
    ```
    """

    add_layer_columns: bool = True
    drop_duplicates: Union[bool, list[str], None] = None
    drop_source_columns: Union[bool, None] = None
    chain: Union[SparkChain, None] = None
    expectations: list[PipelineNodeExpectation] = []
    id: Union[str, None] = None
    layer: Literal["BRONZE", "SILVER", "GOLD"] = None
    primary_key: str = None
    sink: Union[DataSinksUnion, None] = None
    source: DataSourcesUnion
    timestamp_key: str = None
    _df: Any = None

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
        # if self.layer == "GOLD":
        #     if self.drop_source_columns is None:
        #         self.drop_source_columns = False
        #     if self.drop_duplicates is not None:
        #         self.drop_duplicates = False
        #

        # Genera node id
        if self.id is None:
            self.id = str(uuid.uuid4())

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def is_from_cdc(self) -> bool:
        """If `True` CDC source is used to build the table"""
        if self.source is None:
            return False
        else:
            return self.source.is_cdc

    @property
    def layer_spark_chain(self):
        nodes = []

        if self.layer == "BRONZE":
            if self.add_layer_columns:
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

            if self.add_layer_columns:
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
            if self.add_layer_columns:
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

        if self.drop_source_columns and self.chain:
            nodes += [
                SparkChainNode(
                    spark_func_name="drop",
                    spark_func_args=[
                        c
                        for c in self.chain.columns[0]
                        if c not in ["_bronze_at", "_silver_at", "_gold_at"]
                    ],
                )
            ]

        if len(nodes) == 0:
            return None

        return SparkChain(nodes=nodes)

    def execute(
        self,
        spark: SparkSession = None,
        udfs: list[Callable] = None,
        df: AnyDataFrame = None,
    ) -> AnyDataFrame:
        """
        Execute pipeline node

        Parameters
        ----------
        spark: SparkSession
            Spark session
        udfs:
            User-defined functions
        df:
            DataFrame generated

        Returns
        -------
        :
            output Spark DataFrame
        """
        logger.info(f"Applying {self.layer} transformations")

        # Read Source
        if not isinstance(self.source, str):
            df = self.source.read(spark)

        if self.source.is_cdc:
            pass
            # TODO: Apply SCD transformations
            #       Best strategy is probably to build a spark dataframe function and add a node in the chain with
            #       that function
            # https://iterationinsights.com/article/how-to-implement-slowly-changing-dimensions-scd-type-2-using-delta-table
            # https://www.linkedin.com/pulse/implementing-slowly-changing-dimension-2-using-lau-johansson-yemxf/

        # Apply chain
        if self.chain:
            df = self.chain.execute(df, udfs=udfs)

        # Apply layer-specific chain
        if self.layer_spark_chain:
            df = self.layer_spark_chain.execute(df, udfs=udfs)

        # Output to sink
        if self.sink:
            self.sink.write(df)

        # Save DataFrame
        self._df = df

        return df
