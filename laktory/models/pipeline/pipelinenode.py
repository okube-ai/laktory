import os
import shutil
import warnings
from pathlib import Path
from typing import Any

import narwhals as nw
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.enums import STREAMING_BACKENDS
from laktory.enums import DataFrameBackends
from laktory.exceptions import DataQualityExpectationsNotSupported
from laktory.models.basemodel import BaseModel
from laktory.models.dataframe.dataframetransformer import DataFrameTransformer
from laktory.models.dataquality.expectation import DataQualityExpectation
from laktory.models.datasinks import DataSinksUnion
from laktory.models.datasinks import TableDataSink
from laktory.models.datasources import BaseDataSource
from laktory.models.datasources import DataSourcesUnion
from laktory.models.datasources import PipelineNodeDataSource
from laktory.models.datasources import TableDataSource
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.typing import AnyFrame

logger = get_logger(__name__)


class PipelineNode(BaseModel, PipelineChild):
    """
    Pipeline base component generating a DataFrame by reading a data source and
    applying a transformer (chain of dataframe transformations). Optional
    output to a data sink.

    Examples
    --------
    A node reading stock prices data from a CSV file and writing a DataFrame
    as a parquet file.
    ```py
    import io

    import laktory as lk

    node_yaml = '''
        name: brz_stock_prices
        source:
          path: "./events/stock_prices/"
          format: JSON
        sinks:
        - path: ./tables/brz_stock_prices/
          format: PARQUET
    '''

    node = lk.models.PipelineNode.model_validate_yaml(io.StringIO(node_yaml))

    # node.execute()
    ```

    A node reading stock prices from an upstream node and writing a DataFrame
    to a data table.
    ```py
    import io

    import laktory as lk

    node_yaml = '''
        name: slv_stock_prices
        source:
          node_name: brz_stock_prices
        sinks:
        - schema_name: finance
          table_name: slv_stock_prices
        transformer:
          nodes:
          - expr: |
                SELECT
                  data.created_at AS created_at,
                  data.symbol AS symbol,
                  data.open AS open,
                  data.close AS close,
                  data.high AS high,
                  data.low AS low,
                  data.volume AS volume
                FROM
                  {df}
          - func_name: drop_duplicates
            func_kwargs:
              subset:
                - symbol
                - timestamp
    '''

    node = lk.models.PipelineNode.model_validate_yaml(io.StringIO(node_yaml))

    # node.execute()
    ```
    """

    dlt_template: str | None = Field(
        "DEFAULT",
        description="Specify which template (notebook) to use when Delta Live Tables is selected as the orchestrator.",
    )
    expectations: list[DataQualityExpectation] = Field(
        [],
        description="List of data expectations. Can trigger warnings, drop invalid records or fail a pipeline.",
    )
    expectations_checkpoint_path: str = Field(
        None,
        description="Path to which the checkpoint file for which expectations on a streaming dataframe should be written.",
    )
    name: str = Field(..., description="Name given to the node.")
    primary_keys: list[str] = Field(
        None,
        description="""
    A list of column names that uniquely identify each row in the
    DataFrame. These columns are used to:
        - Document the uniqueness constraints of the node's output data.
        - Define the default primary keys for sinks CDC merge operations
        - Referenced in expectations and unit tests.
    While optional, specifying `primary_keys` helps enforce data integrity
    and ensures that downstream operations, such as deduplication, are
    consistent and reliable.
    """,
    )
    source: DataSourcesUnion | None = Field(
        None, description="Definition of the data source(s)"
    )
    sinks: list[DataSinksUnion] = Field(
        None,
        description="Definition of the data sink(s). Set `is_quarantine` to True to store node quarantine DataFrame.",
    )
    transformer: DataFrameTransformer = Field(
        None,
        description="Data transformations applied between the source and the sink(s).",
    )
    root_path: str = Field(
        None,
        description="Location of the pipeline node root used to store logs, metrics and checkpoints.",
    )
    _stage_df: Any = None
    _output_df: Any = None
    _quarantine_df: Any = None

    @field_validator("root_path", "expectations_checkpoint_path", mode="before")
    @classmethod
    def posixpath_to_string(cls, value: Any) -> Any:
        if isinstance(value, Path):
            value = str(value)
        return value

    @model_validator(mode="after")
    def push_primary_keys(self) -> Any:
        # Assign primary keys
        if self.primary_keys and self.sinks:
            for s in self.sinks:
                if s.is_cdc:
                    if s.merge_cdc_options.primary_keys is None:
                        s.merge_cdc_options.primary_keys = self.primary_keys

        return self

    @model_validator(mode="after")
    def validate_expectations(self):
        if self.source and self.source.as_stream:
            # Expectations type
            for e in self.expectations:
                if not e.is_streaming_compatible:
                    raise DataQualityExpectationsNotSupported(e, self)

            if self.expectations and self._expectations_checkpoint_path is None:
                warnings.warn(
                    f"Node '{self.name}' requires `expectations_checkpoint_location` specified unless Delta Live Tables is selected as an orchestrator and expectations are compatbile with DLT."
                )

        return self

    @model_validator(mode="after")
    def validate_view(self):
        if not self.is_view:
            return self

        # Validate Source
        if self.source:
            if not (
                isinstance(self.source, TableDataSource)
                or isinstance(self.source, PipelineNodeDataSource)
            ):
                raise ValueError(
                    "VIEW sink only supports Table or Pipeline Node with Table sink Data Source"
                )

            if self.source.as_stream:
                raise ValueError("VIEW sink does not support stream read.")

        # Validate Sinks
        m = f"node '{self.name}': "
        for s in self.sinks:
            if s.table_type != "VIEW":
                raise ValueError(
                    f"{m}If one pipeline node sink is a VIEW, all of them must be a view."
                )

        # Validate Expectations
        if self.expectations:
            raise ValueError(f"{m}Expectations not supported for a view sink.")

        return self

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return [
            "source",
            "data_sources",
            "expectations",
            "transformer",
            "sinks",
        ]

    # ----------------------------------------------------------------------- #
    # Orchestrator                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def is_orchestrator_dlt(self) -> bool:
        """If `True`, pipeline node is used in the context of a DLT pipeline"""
        pl = self.parent_pipeline
        if pl:
            return pl.is_orchestrator_dlt
        return False

    @property
    def is_dlt_run(self) -> bool:
        if not self.is_orchestrator_dlt:
            return False
        from laktory.dlt import is_debug

        return not is_debug()

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def _root_path(self) -> Path:
        if self.root_path:
            return Path(self.root_path)

        node = self.parent_pipeline
        if node and node._root_path:
            return node._root_path / self.name

        return Path(settings.laktory_root) / self.name

    # ----------------------------------------------------------------------- #
    # Outputs and Sinks                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def is_view(self) -> bool:
        if not self.sinks:
            return False
        is_view = False
        for s in self.sinks:
            if isinstance(s, TableDataSink) and s.table_type == "VIEW":
                is_view = True
                break
        return is_view

    @property
    def stage_df(self) -> AnyFrame:
        """
        Dataframe resulting from reading source and applying transformer, before data quality checks are applied.
        """
        return self._stage_df

    @property
    def output_df(self) -> AnyFrame:
        """
        Dataframe resulting from reading source, applying transformer and dropping rows not meeting data quality
        expectations.
        """
        return self._output_df

    @property
    def quarantine_df(self) -> AnyFrame:
        """
        DataFrame storing `stage_df` rows not meeting data quality expectations.
        """
        return self._quarantine_df

    @property
    def output_sinks(self) -> list[DataSinksUnion]:
        """List of sinks writing the output DataFrame"""
        sinks = []
        if self.sinks:
            for s in self.sinks:
                if not s.is_quarantine:
                    sinks += [s]
        return sinks

    @property
    def quarantine_sinks(self) -> list[DataSinksUnion]:
        """List of sinks writing the quarantine DataFrame"""
        sinks = []
        if self.sinks:
            for s in self.sinks:
                if s.is_quarantine:
                    sinks += [s]
        return sinks

    @property
    def all_sinks(self):
        """List of all sinks (output and quarantine)."""
        return self.output_sinks + self.quarantine_sinks

    @property
    def sinks_count(self) -> int:
        """Total number of sinks."""
        return len(self.all_sinks)

    @property
    def has_output_sinks(self) -> bool:
        """`True` if node has at least one output sink."""
        return len(self.output_sinks) > 0

    # @property
    # def has_quarantine_sinks(self) -> bool:
    #     """`True` if node has at least one quarantine sink."""
    #     return len(self.quarantine_sinks) > 0

    @property
    def has_sinks(self) -> bool:
        """`True` if node has at least one sink."""
        return self.sinks_count > 0

    @property
    def primary_sink(self) -> DataSinksUnion | None:
        """Primary output sink used as a source for downstream nodes."""
        if not self.has_output_sinks:
            return None

        return self.output_sinks[0]

    # ----------------------------------------------------------------------- #
    # Expectations                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def dlt_warning_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_dlt_compatible and e.action == "WARN":
                expectations[e.name] = e.expr.expr
        return expectations

    @property
    def dlt_drop_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_dlt_compatible and e.action in ["DROP", "QUARANTINE"]:
                expectations[e.name] = e.expr.expr
        return expectations

    @property
    def dlt_fail_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_dlt_compatible and e.action == "FAIL":
                expectations[e.name] = e.expr.expr
        return expectations

    @property
    def _expectations_checkpoint_path(self) -> Path | None:
        if self.expectations_checkpoint_path:
            return Path(self.expectations_checkpoint_path)

        if self._root_path:
            return Path(self._root_path) / "checkpoints/expectations"

        return None

    @property
    def checks(self):
        return [e.check for e in self.expectations]

    # ----------------------------------------------------------------------- #
    # Upstream Nodes                                                          #
    # ----------------------------------------------------------------------- #

    @property
    def upstream_node_names(self) -> list[str]:
        """Pipeline node names required to execute current node."""
        from laktory.models.datasources.pipelinenodedatasource import (
            PipelineNodeDataSource,
        )

        names = []

        if isinstance(self.source, PipelineNodeDataSource):
            names += [self.source.node_name]

        if self.transformer:
            names += self.transformer.upstream_node_names

        if self.sinks:
            for s in self.sinks:
                names += s.upstream_node_names

        return names

    # ----------------------------------------------------------------------- #
    # Data Sources                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def data_sources(self) -> list[BaseDataSource]:
        """Get all sources feeding the pipeline node"""
        sources = []

        if isinstance(self.source, BaseDataSource):
            sources += [self.source]

        if self.transformer:
            sources += self.transformer.data_sources

        if self.sinks:
            for s in self.sinks:
                sources += s.data_sources

        return sources

    # ----------------------------------------------------------------------- #
    # Execution                                                               #
    # ----------------------------------------------------------------------- #

    def purge(self, spark=None):
        # TODO: Now that sink switch to overwrite when sink does not exists or when
        # a full refresh is requested, the purge method should not delete the data
        # by default, but only the checkpoints. Also consider truncating the table
        # instead of dropping it.

        logger.info(f"Purging pipeline node {self.name}")
        if self.has_sinks:
            for s in self.sinks:
                s.purge(spark=spark)
        if self._expectations_checkpoint_path:
            if os.path.exists(self._expectations_checkpoint_path):
                logger.info(
                    f"Deleting expectations checkpoint at {self._expectations_checkpoint_path}",
                )
                shutil.rmtree(self._expectations_checkpoint_path)

            if spark is None:
                return

            try:
                from pyspark.dbutils import DBUtils
            except ModuleNotFoundError:
                return

            dbutils = DBUtils(spark)

            _path = self._expectations_checkpoint_path.as_posix()
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

    def execute(
        self,
        apply_transformer: bool = True,
        write_sinks: bool = True,
        full_refresh: bool = False,
        named_dfs: dict[str, AnyFrame] = None,
    ) -> AnyFrame:
        """
        Execute pipeline node by:

        - Reading the source
        - Applying the user defined (and layer-specific if applicable) transformations
        - Checking expectations
        - Writing the sinks

        Parameters
        ----------
        apply_transformer:
            Flag to apply transformer in the execution
        write_sinks:
            Flag to include writing sink in the execution
        full_refresh:
            If `True` dataframe will be completely re-processed by deleting
            existing data and checkpoint before processing.
        named_dfs:
            Named DataFrame passed to transformer nodes

        Returns
        -------
        :
            output Spark DataFrame
        """
        logger.info(f"Executing pipeline node {self.name}")

        # Parse DLT
        if self.is_orchestrator_dlt:
            logger.info("DLT orchestrator selected. Sinks writing will be skipped.")
            write_sinks = False
            full_refresh = False

        # Refresh
        if full_refresh:
            self.purge()

        # Read Source
        self._stage_df = None
        if self.source:
            self._stage_df = self.source.read()

        # Apply transformer
        if named_dfs is None:
            named_dfs = {}
        if apply_transformer and self.transformer:
            self._stage_df = self.transformer.execute(
                self._stage_df, named_dfs=named_dfs
            )

        # Check expectations
        self._output_df = self._stage_df
        self._quarantine_df = None
        self.check_expectations()

        # Output and Quarantine to Sinks
        if write_sinks:
            for s in self.output_sinks:
                if self.is_view:
                    s.write()
                    self._output_df = s.as_source().read()
                else:
                    s.write(self._output_df, full_refresh=full_refresh)

            if self._quarantine_df is not None:
                for s in self.quarantine_sinks:
                    s.write(self._quarantine_df, full_refresh=full_refresh)

        return self._output_df

    def check_expectations(self):
        """
        Check expectations, raise errors, warnings where required and build
        filtered and quarantine DataFrames.

        Some actions have to be disabled when selected orchestrator is
        Databricks DLT:

        * Raising error on Failure when expectation is supported by DLT
        * Dropping rows when expectation is supported by DLT
        """

        # Data Quality Checks
        qfilter = None  # Quarantine filter
        kfilter = None  # Keep filter
        if self._stage_df is None:
            # Node without source or transformer
            return
        is_streaming = getattr(nw.to_native(self._stage_df), "isStreaming", False)
        if not self.expectations:
            return

        logger.info("Checking Data Quality Expectations")

        def _batch_check(df, node):
            for e in node.expectations:
                is_dlt_managed = node.is_dlt_run and e.is_dlt_compatible

                # Run Check
                if not is_dlt_managed:
                    e.run_check(
                        df,
                        raise_or_warn=True,
                        node=node,
                    )

        def _stream_check(batch_df, batch_id, node):
            _batch_check(
                batch_df,
                node,
            )

        # Warn or Fail
        if is_streaming and self.is_dlt_run:
            # TODO: Enable when DLT supports foreachBatch (in case some expectations are not supported by DLT)
            pass

        elif is_streaming:
            backend = DataFrameBackends.from_df(self._stage_df)
            if backend not in STREAMING_BACKENDS:
                raise TypeError(
                    f"DataFrame backend {backend} is not supported for streaming operations"
                )

            if self._expectations_checkpoint_path is None:
                raise ValueError(
                    f"Expectations Checkpoint not specified for node '{self.name}'"
                )

            # TODO: Refactor for backend other than spark
            query = (
                self._stage_df.to_native()
                .writeStream.foreachBatch(
                    lambda batch_df, batch_id: _stream_check(
                        nw.from_native(batch_df), batch_id, self
                    )
                )
                .trigger(availableNow=True)
                .options(
                    checkpointLocation=self._expectations_checkpoint_path,
                )
                .start()
            )
            query.awaitTermination()

        else:
            _batch_check(
                self._stage_df,
                self,
            )

        # Build Filters
        for e in self.expectations:
            is_dlt_managed = self.is_dlt_run and e.is_dlt_compatible

            # Update Keep Filter
            if not is_dlt_managed:
                _filter = e.keep_filter
                if _filter is not None:
                    if kfilter is None:
                        kfilter = _filter
                    else:
                        kfilter = kfilter & _filter

            # Update Quarantine Filter
            _filter = e.quarantine_filter
            if _filter is not None:
                if qfilter is None:
                    qfilter = _filter
                else:
                    qfilter = qfilter & _filter

        if qfilter is not None:
            logger.info("Building quarantine DataFrame")
            self._quarantine_df = self._stage_df.filter(qfilter)
        else:
            self._quarantine_df = self._stage_df  # .filter("False")

        if kfilter is not None:
            logger.info("Dropping invalid rows")
            self._output_df = self._stage_df.filter(kfilter)
        else:
            self._output_df = self._stage_df
