import uuid
import os
import shutil
from typing import Any
from typing import Callable
from typing import Literal
from typing import Union
import warnings
from pydantic import model_validator

from laktory.constants import DEFAULT_DFTYPE
from laktory.exceptions import DataQualityExpectationsNotSupported
from laktory.models.basemodel import BaseModel
from laktory.models.datasources import DataSourcesUnion
from laktory.models.datasources import BaseDataSource
from laktory.models.datasinks import DataSinksUnion
from laktory.models.dataquality.expectation import DataQualityExpectation
from laktory.models.transformers.basechain import BaseChain
from laktory.models.transformers.sparkchain import SparkChain
from laktory.models.transformers.sparkchainnode import SparkChainNode
from laktory.models.transformers.polarschain import PolarsChain
from laktory.models.transformers.polarschainnode import PolarsChainNode
from laktory.spark import SparkSession
from laktory.types import AnyDataFrame
from laktory._logger import get_logger

logger = get_logger(__name__)


class PipelineNode(BaseModel):
    """
    Pipeline base component generating a DataFrame by reading a data source and
    applying a transformer (chain of dataframe transformations). Optional
    output to a data sink. Some basic transformations are also natively
    supported.

    Attributes
    ----------
    add_layer_columns:
        If `True` and `layer` not `None` layer-specific columns like timestamps
        are added to the resulting DataFrame.
    dlt_template:
        Specify which template (notebook) to use if pipeline is run with
        Databricks Delta Live Tables. If `None` default laktory template
        notebook is used.
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
    layer:
        Layer in the medallion architecture
    name:
        Name given to the node. Required to reference a node in a data source.
    primary_key:
        Name of the column storing a unique identifier for each row. It is used
        by the node to drop duplicated rows.
    source:
        Definition of the data source
    sink:
        Definition of the data sink
    transformer:
        Spark or Polars chain defining the data transformations applied to the
        data source.
    timestamp_key:
        Name of the column storing a timestamp associated with each row. It is
        used as the default column by the builder when creating watermarks.


    Examples
    --------
    A node reading stock prices data from a CSV file and writing a DataFrame
    to disk as a parquet file.
    ```py
    import io
    from laktory import models

    node_yaml = '''
        name: brz_stock_prices
        layer: BRONZE
        source:
            format: CSV
            path: ./raw/brz_stock_prices.csv
        sink:
            format: PARQUET
            mode: OVERWRITE
            path: ./dataframes/brz_stock_prices
    '''

    node = models.PipelineNode.model_validate_yaml(io.StringIO(node_yaml))

    # node.execute(spark)
    ```

    A node reading stock prices from an upstream node and writing a DataFrame
    to a data table.
    ```py
    import io
    from laktory import models

    node_yaml = '''
        name: slv_stock_prices
        layer: SILVER
        source:
          node_name: brz_stock_prices
        sink:
          catalog_name: hive_metastore
          schema_name: default
          table_name: slv_stock_prices
        transformer:
          nodes:
          - with_column:
              name: created_at
              type: timestamp
              expr: data.created_at
          - with_column:
              name: symbol
              expr: data.symbol
          - with_column:
              name: close
              type: double
              expr: data.close
    '''

    node = models.PipelineNode.model_validate_yaml(io.StringIO(node_yaml))

    # node.execute(spark)
    ```
    """

    add_layer_columns: bool = True
    dlt_template: Union[str, None] = "DEFAULT"
    dataframe_type: Literal["SPARK", "POLARS"] = DEFAULT_DFTYPE
    description: str = None
    drop_duplicates: Union[bool, list[str], None] = None
    drop_source_columns: Union[bool, None] = None
    transformer: Union[SparkChain, PolarsChain, None] = None
    expectations: list[DataQualityExpectation] = []
    expectations_checkpoint_location: str = None
    layer: Literal["BRONZE", "SILVER", "GOLD"] = None
    name: Union[str, None] = None
    primary_key: str = None
    sink: Union[DataSinksUnion, None] = None
    source: DataSourcesUnion
    timestamp_key: str = None
    _output_df: Any = None
    _quarantine_df: Any = None
    _parent: "Pipeline" = None
    _source_columns: list[str] = []

    @model_validator(mode="before")
    @classmethod
    def push_dftype_before(cls, data: Any) -> Any:
        dftype = data.get("dataframe_type", None)
        if dftype:
            if "source" in data.keys():
                if isinstance(data["source"], dict):
                    data["source"]["dataframe_type"] = data["source"].get(
                        "dataframe_type", dftype
                    )
            if "transformer" in data.keys():
                if isinstance(data["transformer"], dict):
                    data["transformer"]["dataframe_type"] = data["transformer"].get(
                        "dataframe_type", dftype
                    )

        return data

    @model_validator(mode="after")
    def push_dftype_after(self) -> Any:
        dftype = self.user_dftype
        if dftype:
            self.source.dataframe_type = self.source.user_dftype or dftype
            if self.transformer:
                self.transformer.dataframe_type = self.transformer.user_dftype or dftype
                self.transformer.push_dftype()
        return self

    @model_validator(mode="after")
    def default_values(self) -> Any:
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

        # Generate node id
        if self.name is None:
            self.name = str(uuid.uuid4())

        return self

    @model_validator(mode="after")
    def update_children(self) -> Any:

        # Assign node to sources
        for s in self.get_sources():
            s._parent = self

        # Assign node to sinks
        if self.sink:
            self.sink._parent = self

        # Assign node to transformers
        if self.transformer:
            self.transformer._parent = self

        return self

    @model_validator(mode="after")
    def validate_expectations(self):

        if self.source.as_stream:
            # Expectations type
            for e in self.expectations:
                if not e.is_streaming_compatible:
                    raise DataQualityExpectationsNotSupported(e, self)

            if self.expectations and self._expectations_checkpoint_location is None:
                warnings.warn(
                    f"Node '{self.name}' requires `expectations_checkpoint_location` specified unless Delta Live Tables is selected as an orchestrator and expectations are compatbile with DLT."
                )

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def user_dftype(self):
        if "dataframe_type" in self.model_fields_set:
            return self.dataframe_type
        return None

    @property
    def is_orchestrator_dlt(self) -> bool:
        """If `True`, pipeline node is used in the context of a DLT pipeline"""
        is_orchestrator_dlt = False
        if self._parent and self._parent.is_orchestrator_dlt:
            is_orchestrator_dlt = True
        return is_orchestrator_dlt

    @property
    def is_dlt_run(self) -> bool:
        if not self.is_orchestrator_dlt:
            return False
        from laktory.dlt import is_debug

        return not is_debug()

    @property
    def is_from_cdc(self) -> bool:
        """If `True` CDC source is used to build the table"""
        if self.source is None:
            return False
        else:
            return self.source.is_cdc

    @property
    def dlt_warning_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_dlt_compatible and e.action == "WARN":
                expectations[e.name] = e.expr.value
        return expectations

    @property
    def dlt_drop_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_dlt_compatible and e.action in ["DROP", "QUARANTINE"]:
                expectations[e.name] = e.expr.value
        return expectations

    @property
    def dlt_fail_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_dlt_compatible and e.action == "FAIL":
                expectations[e.name] = e.expr.value
        return expectations

    @property
    def _expectations_checkpoint_location(self) -> str:
        checkpoint_location = self.expectations_checkpoint_location
        if checkpoint_location is None and self.sink:
            checkpoint_location = self.sink._checkpoint_location
            if checkpoint_location:
                if checkpoint_location.endswith("/"):
                    checkpoint_location = checkpoint_location[:-1] + "_expectations/"
                else:
                    checkpoint_location = checkpoint_location + "_expectations"
        return checkpoint_location

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
            "source": self.source.table_name,
            "stored_as_scd_type": cdc.scd_type,
            "target": self.sink.table_name,
            "track_history_column_list": cdc.track_history_columns,
            "track_history_except_column_list": cdc.track_history_except_columns,
        }

    @property
    def output_df(self) -> AnyDataFrame:
        """Node Dataframe after reading source and applying transformer."""
        return self._output_df

    @property
    def quarantine_df(self) -> AnyDataFrame:
        """Node Dataframe after reading source and applying transformer."""
        return self._quarantine_df

    @property
    def checks(self):
        return [e.check for e in self.expectations]

    @property
    def layer_spark_chain(self):
        nodes = []

        if self.layer == "BRONZE":
            if self.add_layer_columns:
                nodes += [
                    SparkChainNode(
                        with_column={
                            "name": "_bronze_at",
                            "type": "timestamp",
                            "expr": "F.current_timestamp()",
                        },
                    ),
                ]

        elif self.layer == "SILVER":
            if self.timestamp_key:
                nodes += [
                    SparkChainNode(
                        with_column={
                            "name": "_tstamp",
                            "type": "timestamp",
                            "sql_expr": self.timestamp_key,
                        },
                    )
                ]

            if self.add_layer_columns:
                nodes += [
                    SparkChainNode(
                        with_column={
                            "name": "_silver_at",
                            "type": "timestamp",
                            "expr": "F.current_timestamp()",
                        },
                    )
                ]

        elif self.layer == "GOLD":
            if self.add_layer_columns:
                nodes += [
                    SparkChainNode(
                        with_column={
                            "name": "_gold_at",
                            "type": "timestamp",
                            "expr": "F.current_timestamp()",
                        },
                    )
                ]

        if self.drop_duplicates:
            subset = None
            if isinstance(self.drop_duplicates, list):
                subset = self.drop_duplicates
            elif self.primary_key:
                subset = [self.primary_key]

            nodes += [SparkChainNode(func_name="dropDuplicates", func_args=[subset])]

        if self.drop_source_columns and self.transformer:
            nodes += [
                SparkChainNode(
                    func_name="drop",
                    func_args=[
                        c
                        for c in self._source_columns
                        if c not in ["_bronze_at", "_silver_at", "_gold_at"]
                    ],
                )
            ]

        if len(nodes) == 0:
            return None

        return SparkChain(nodes=nodes)

    @property
    def layer_polars_chain(self):
        nodes = []

        if self.layer == "BRONZE":
            if self.add_layer_columns:
                nodes += [
                    PolarsChainNode(
                        with_column={
                            "name": "_bronze_at",
                            "type": "timestamp",
                            "expr": "pl.expr.laktory.current_timestamp()",
                        },
                    ),
                ]

        elif self.layer == "SILVER":
            if self.timestamp_key:
                nodes += [
                    PolarsChainNode(
                        with_column={
                            "name": "_tstamp",
                            "type": "timestamp",
                            "sql_expr": self.timestamp_key,
                        },
                    )
                ]

            if self.add_layer_columns:
                nodes += [
                    PolarsChainNode(
                        with_column={
                            "name": "_silver_at",
                            "type": "timestamp",
                            "expr": "pl.expr.laktory.current_timestamp()",
                        },
                    )
                ]

        elif self.layer == "GOLD":
            if self.add_layer_columns:
                nodes += [
                    PolarsChainNode(
                        with_column={
                            "name": "_gold_at",
                            "type": "timestamp",
                            "expr": "pl.expr.laktory.current_timestamp()",
                        },
                    )
                ]

        if self.drop_duplicates:
            subset = None
            if isinstance(self.drop_duplicates, list):
                subset = self.drop_duplicates
            elif self.primary_key:
                subset = [self.primary_key]

            nodes += [PolarsChainNode(func_name="unique", func_args=[subset])]

        if self.drop_source_columns and self.transformer:
            nodes += [
                PolarsChainNode(
                    func_name="drop",
                    func_args=[
                        c
                        for c in self._source_columns
                        if c not in ["_bronze_at", "_silver_at", "_gold_at"]
                    ],
                )
            ]

        if len(nodes) == 0:
            return None

        return PolarsChain(nodes=nodes)

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def get_sources(self, cls=BaseDataSource) -> list[BaseDataSource]:
        """Get all sources feeding the pipeline node"""
        sources = []

        if isinstance(self.source, cls):
            sources += [self.source]

        if self.transformer:
            sources += self.transformer.get_sources(cls)

        return sources

    def purge(self, spark=None):
        if self.sink:
            self.sink.purge(spark=spark)
        if self._expectations_checkpoint_location:
            if os.path.exists(self._expectations_checkpoint_location):
                logger.info(
                    f"Deleting expectations checkpoint at {self._expectations_checkpoint_location}",
                )
                shutil.rmtree(self._expectations_checkpoint_location)

    def execute(
        self,
        apply_transformer: bool = True,
        spark: SparkSession = None,
        udfs: list[Callable] = None,
        write_sink: bool = True,
        full_refresh: bool = False,
    ) -> AnyDataFrame:
        """
        Execute pipeline node by:

        - Reading the source
        - Applying the user defined (and layer-specific if applicable) transformations
        - Checking expectations
        - Writing the sink

        Parameters
        ----------
        apply_transformer:
            Flag to apply transformer in the execution
        spark: SparkSession
            Spark session
        udfs:
            User-defined functions
        write_sink:
            Flag to include writing sink in the execution
        full_refresh:
            If `True` dataframe will be completely re-processed by deleting
            existing data and checkpoint before processing.

        Returns
        -------
        :
            output Spark DataFrame
        """
        logger.info(f"Executing pipeline node {self.name}")

        # Parse DLT
        if self.is_orchestrator_dlt:
            logger.info("DLT orchestrator selected. Sinks writing will be skipped.")
            write_sink = False
            full_refresh = False

        # Refresh
        if full_refresh:
            self.purge(spark)

        # Read Source
        self._output_df = self.source.read(spark)

        # Save source
        self._source_columns = self._output_df.columns

        if self.source.is_cdc and not self.is_dlt_run:
            pass
            # TODO: Apply SCD transformations
            #       Best strategy is probably to build a spark dataframe function and add a node in the chain with
            #       that function
            # https://iterationinsights.com/article/how-to-implement-slowly-changing-dimensions-scd-type-2-using-delta-table
            # https://www.linkedin.com/pulse/implementing-slowly-changing-dimension-2-using-lau-johansson-yemxf/

        # Apply transformer
        if apply_transformer:

            if "spark" in str(type(self._output_df)).lower():
                dftype = "spark"
            elif "polars" in str(type(self._output_df)).lower():
                dftype = "polars"
            else:
                raise ValueError("DataFrame type not supported")

            # Set Transformer
            transformer = self.transformer
            if transformer is None:
                if dftype == "spark":
                    transformer = SparkChain(nodes=[])
                elif dftype == "polars":
                    transformer = PolarsChain(nodes=[])
            else:
                transformer = transformer.model_copy()

            # Add layer-specific chain nodes
            if dftype == "spark" and self.layer_spark_chain:
                transformer.nodes += self.layer_spark_chain.nodes
            elif dftype == "polars" and self.layer_polars_chain:
                transformer.nodes += self.layer_polars_chain.nodes

            if transformer.nodes:
                self._output_df = transformer.execute(self._output_df, udfs=udfs)

        # Check expectations
        self.check_expectations()

        # Output to sink
        if write_sink and self.sink:
            self.sink.write(self._output_df)

        return self._output_df

    def check_expectations(self):
        """
        Check expectations, raise errors, warnings where required and build
        filtered and quarantine DataFrames.

        Some actions have to be disabled when selected orchestrator is
        Databricks DLT:
            - Raising error on Failure when expectation is supported by DLT
            - Dropping rows when expectation is supported by DLT
        """

        # Data Quality Checks
        is_streaming = getattr(self._output_df, "isStreaming", False)
        filters = {"keep": None, "quarantine": None}
        if not self.expectations:
            return

        logger.info(f"Checking Data Quality Expectations")

        def _batch_check(
            df,
            node,
            filters,
        ):
            for e in node.expectations:

                is_dlt_managed = node.is_dlt_run and e.is_dlt_compatible

                # Run Check
                if not is_dlt_managed:
                    e.run_check(
                        df,
                        raise_or_warn=True,
                        node=node,
                    )

                # Update Keep Filter
                if not is_dlt_managed:
                    _filter = e.keep_filter
                    if _filter is not None:
                        if filters["keep"] is None:
                            filters["keep"] = _filter
                        else:
                            filters["keep"] = filters["keep"] & _filter

                # Update Quarantine Filter
                _filter = e.quarantine_filter
                if _filter is not None:
                    if filters["quarantine"] is None:
                        filters["quarantine"] = _filter
                    else:
                        filters["quarantine"] = filters["quarantine"] & _filter

        def _stream_check(batch_df, batch_id, node):
            _batch_check(
                batch_df,
                node,
                filters,
            )

        if is_streaming:
            if self._expectations_checkpoint_location is None:
                raise ValueError(
                    f"Expectations Checkpoint not specified for node '{self.name}'"
                )
            query = (
                self._output_df.writeStream.foreachBatch(
                    lambda batch_df, batch_id: _stream_check(batch_df, batch_id, self)
                )
                .trigger(availableNow=True)
                .options(
                    checkpointLocation=self._expectations_checkpoint_location,
                )
                .start()
            )
            query.awaitTermination()

        else:
            _batch_check(
                self._output_df,
                self,
                filters,
            )

        if filters["quarantine"] is not None:
            logger.info(f"Building quarantine DataFrame")
            self._quarantine_df = self._output_df.filter(filters["quarantine"])

        if filters["keep"] is not None:
            logger.info(f"Dropping invalid rows")
            self._output_df = self._output_df.filter(filters["keep"])
