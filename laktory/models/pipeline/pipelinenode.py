import importlib
import os
import shutil
import warnings
from pathlib import Path
from typing import Any

import narwhals as nw
from pydantic import AliasChoices
from pydantic import Field
from pydantic import ValidationError
from pydantic import computed_field
from pydantic import field_serializer
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
from laktory.models.pipelinechild import PipelineChild
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
        sources:
        - path: "./events/stock_prices/"
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
        sources:
        - node_name: brz_stock_prices
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
    References
    ----------
    * [Data Pipeline](https://www.laktory.ai/concepts/pipeline/)
    """

    ldp_template: str | None = Field(
        "DEFAULT",
        description="Specify which template (notebook) to use when Lakeflow Declarative Pipeline is selected as the orchestrator.",
    )
    execution_task_name_: str = Field(
        None,
        description="""
        Execution task name when orchestrator (such as Databricks Jobs and Airflow) supports multi-tasks execution. 
        Nodes with the same task name will be executed together in a single task. If `None` is provided, node will be 
        executed under `node-{node-name}`.
        """,
        validation_alias=AliasChoices("execution_task_name", "execution_task_name_"),
    )
    comment: str = Field(
        None,
        description="Comment for the associated table or view",
    )
    expectations: list[DataQualityExpectation] = Field(
        [],
        description="List of data expectations. Can trigger warnings, drop invalid records or fail a pipeline.",
    )
    expectations_checkpoint_path_: str | Path = Field(
        None,
        description="Path to which the checkpoint file for which expectations on a streaming dataframe should be written.",
        validation_alias=AliasChoices(
            "expectations_checkpoint_path", "expectations_checkpoint_path_"
        ),
        exclude=True,
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
    depends_on: list[str] = Field(
        [],
        description="Pipeline node names that must complete before this node executes. Use for DAG ordering when no data flows between nodes.",
    )
    sources: list[DataSourcesUnion] = Field(
        [],
        description="Data sources for this node. The first entry is the primary input fed into the transformer as `{df}`. Assign a `name` to each source to reference it as `{sources.name}` in transformer expressions.",
    )
    sinks: list[DataSinksUnion] = Field(
        [],
        description="Definition of the data sink(s). Set `is_quarantine` to True to store node quarantine DataFrame.",
    )
    tags: list[str] = Field([], description="Node tags for selective execution.")
    time_column: str | None = Field(
        None,
        description="""
        The name of the column that represents the timestamp or temporal dimension in the DataFrame. This column is 
        used to:
            - Document the time-based ordering and filtering semantics of the node’s output data.
            - Enable time-aware operations such as point-in-time joins, incremental processing, and time-series analysis.
            - Serve as a reference in expectations, unit tests, and feature engineering workflows.
        While optional, specifying time_column helps ensure consistency in time-based logic and improves the 
        reliability of downstream operations that depend on temporal alignment.        
        """,
    )
    transformer: DataFrameTransformer = Field(
        None,
        description="Data transformations applied between the source and the sink(s).",
    )
    root_path_: str | Path = Field(
        None,
        description="Location of the pipeline node root used to store logs, metrics and checkpoints.",
        validation_alias=AliasChoices("root_path", "root_path_"),
        exclude=True,
    )
    _stage_df: Any = None
    _output_df: Any = None
    _quarantine_df: Any = None

    @model_validator(mode="before")
    @classmethod
    def _check_deprecated_dlt_template(cls, data):
        if isinstance(data, dict) and "dlt_template" in data:
            raise ValueError(
                "'dlt_template' was renamed to 'ldp_template' in v0.12. Please update your YAML."
            )
        return data

    @model_validator(mode="before")
    @classmethod
    def _migrate_source(cls, data):
        """Accept legacy `source` key and normalise it to `sources: [...]`."""
        if not isinstance(data, dict):
            return data
        if "source" in data and "sources" not in data:
            source = data.pop("source")
            if source is not None:
                data["sources"] = [source]
        return data

    @field_validator("sources", mode="before")
    @classmethod
    def _validate_sources_types(cls, v):
        from laktory.models.datasources.customdatasource import CustomDataSource
        from laktory.models.datasources.dataframedatasource import DataFrameDataSource
        from laktory.models.datasources.filedatasource import FileDataSource
        from laktory.models.datasources.hivemetastoredatasource import (
            HiveMetastoreDataSource,
        )
        from laktory.models.datasources.pipelinenodedatasource import (
            PipelineNodeDataSource,
        )
        from laktory.models.datasources.unitycatalogdatasource import (
            UnityCatalogDataSource,
        )

        _source_map = {
            "CUSTOM": CustomDataSource,
            "DATAFRAME": DataFrameDataSource,
            "FILE": FileDataSource,
            "HIVE_METASTORE": HiveMetastoreDataSource,
            "PIPELINE_NODE": PipelineNodeDataSource,
            "UNITY_CATALOG": UnityCatalogDataSource,
        }

        if not isinstance(v, list):
            return v

        result = []
        for i, item in enumerate(v):
            if not isinstance(item, dict):
                result.append(item)
                continue
            source_type = item.get("type")
            if source_type is None:
                result.append(item)
                continue
            target_cls = _source_map.get(source_type)
            if target_cls is None:
                result.append(item)
                continue
            try:
                result.append(target_cls.model_validate(item))
            except ValidationError as e:
                raise ValueError(f"sources[{i}]: {e}") from None
        return result

    @field_validator("sinks", mode="before")
    @classmethod
    def _validate_sink_types(cls, v):
        if not isinstance(v, list):
            return v
        from laktory.models.datasinks.filedatasink import FileDataSink
        from laktory.models.datasinks.hivemetastoredatasink import HiveMetastoreDataSink
        from laktory.models.datasinks.pipelineviewdatasink import PipelineViewDataSink
        from laktory.models.datasinks.unitycatalogdatasink import UnityCatalogDataSink

        _sink_map = {
            "FILE": FileDataSink,
            "HIVE_METASTORE": HiveMetastoreDataSink,
            "PIPELINE_VIEW": PipelineViewDataSink,
            "UNITY_CATALOG": UnityCatalogDataSink,
        }
        result = []
        for i, item in enumerate(v):
            if not isinstance(item, dict):
                result.append(item)
                continue
            sink_type = item.get("type")
            target_cls = _sink_map.get(sink_type)
            if target_cls is None:
                result.append(item)
                continue
            try:
                result.append(target_cls.model_validate(item))
            except ValidationError as e:
                raise ValueError(f"sinks[{i}]: {e}") from None
        return result

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
        if self.has_streaming_source:
            # Expectations type
            for e in self.expectations:
                if not e.is_streaming_compatible:
                    raise DataQualityExpectationsNotSupported(e, self)

            if self.expectations and self.expectations_checkpoint_path is None:
                warnings.warn(
                    f"Node '{self.name}' requires `expectations_checkpoint_location` specified unless Databricks pipeline is selected as an orchestrator and expectations are compatible with Declarative Pipelines."
                )

        return self

    @model_validator(mode="after")
    def validate_view(self):
        if not self.is_view:
            return self

        # Validate Sources
        for isource, source in enumerate(self.sources):
            label = source.name or str(isource)
            if not (
                isinstance(source, TableDataSource)
                or isinstance(source, PipelineNodeDataSource)
            ):
                raise ValueError(
                    f"Source '{label}' for node '{self.name}' is not supported. VIEW sink only supports Table or Pipeline Node with a Table sink"
                )

            if source.as_stream:
                raise ValueError(
                    f"Source '{label}' for node '{self.name}' is not supported. VIEW sink does not support streaming sources."
                )

        # Validate Sinks
        m = f"node '{self.name}': "

        # Validate Transformer
        if not self.transformer:
            raise ValueError(
                f"{m}VIEW sink requires a transformer with single node expressed as SQL statement."
            )
        if not self.transformer.is_sql_expressible:
            raise ValueError(
                f"{m}VIEW sink requires a transformer with single node expressed as SQL statement."
            )

        # Validate Expectations
        if self.expectations:
            raise ValueError(f"{m}Expectations not supported for a view sink.")

        return self

    @property
    def has_streaming_source(self) -> bool:
        """True if any declared source is read as a stream."""
        return any(s.as_stream for s in self.sources)

    @property
    def view_definition(self):
        """Transformer View Definition (when applicable)"""
        if self.transformer is None:
            return None
        return self.transformer.view_definition

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return [
            "sources",
            "data_sources",
            "expectations",
            "transformer",
            "sinks",
        ]

    def _inject_vars_objs(self) -> dict:
        return {"pipeline_node": self}

    # ----------------------------------------------------------------------- #
    # Orchestrator                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def execution_task_name(self) -> str:
        if self.execution_task_name_:
            return self.execution_task_name_
        return f"node-{self.name}"

    @property
    def is_orchestrator_ldp(self) -> bool:
        """If `True`, pipeline node is used in the context of a Lakeflow Declarative Pipeline"""
        pl = self.parent_pipeline
        if pl:
            return pl.is_orchestrator_ldp
        return False

    @property
    def is_orchestrator_sdp(self) -> bool:
        """If `True`, pipeline node is used in the context of a Spark Declarative Pipeline"""
        pl = self.parent_pipeline
        if pl:
            return pl.is_orchestrator_sdp
        return False

    @property
    def is_ldp_execute(self) -> bool:
        if not self.is_orchestrator_ldp:
            return False
        from laktory import is_ldp_execute

        return is_ldp_execute()

    @property
    def is_sdp_execute(self) -> bool:
        if not self.is_orchestrator_sdp:
            return False
        from laktory import is_sdp_execute

        return is_sdp_execute()

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @computed_field(description="root_path")
    @property
    def root_path(self) -> Path:
        if self.root_path_:
            return Path(self.root_path_)

        pl = self.parent_pipeline
        if pl and pl.root_path:
            return pl.root_path / self.name

        return Path(settings.runtime_root) / self.name

    @field_serializer("root_path", "expectations_checkpoint_path", when_used="json")
    def serialize_path(self, value: Path) -> str:
        return value.as_posix()

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
    def is_sql_expressible(self) -> bool:
        """`True` if the node can be executed on a SQL warehouse."""
        if self.transformer is None:
            return False
        if not self.sinks:
            return False
        return self.transformer.is_sql_expressible and all(
            s.is_sql_expressible for s in self.sinks
        )

    @property
    def sql_statements(self) -> list[str]:
        """
        Generate one SQL statement per sink, suitable for execution on a SQL warehouse.

        Raises ValueError if the node is not SQL-expressible.
        """
        if not self.is_sql_expressible:
            raise ValueError(
                f"Node '{self.name}' is not SQL-expressible. "
                "Transformer must be a single SQL DataFrameExpr and all sinks must be "
                "TableDataSink with mode OVERWRITE, APPEND, or None (VIEW)."
            )

        select_sql = self.view_definition.to_sql()
        statements = []
        for sink in self.sinks:
            if sink.table_type == "VIEW":
                stmt = f"CREATE OR REPLACE VIEW {sink.full_name} AS\n{select_sql}"
            elif sink.mode == "APPEND":
                stmt = f"INSERT INTO {sink.full_name}\n{select_sql}"
            else:
                stmt = f"CREATE OR REPLACE TABLE {sink.full_name} AS\n{select_sql}"
            statements.append(stmt)
        return statements

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
    def ldp_warning_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_ldp_compatible and e.action == "WARN":
                expectations[e.name] = e.expr.expr
        return expectations

    @property
    def ldp_drop_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_ldp_compatible and e.action in ["DROP", "QUARANTINE"]:
                expectations[e.name] = e.expr.expr
        return expectations

    @property
    def ldp_fail_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_ldp_compatible and e.action == "FAIL":
                expectations[e.name] = e.expr.expr
        return expectations

    @property
    def sdp_warning_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_sdp_compatible and e.action == "WARN":
                expectations[e.name] = e.expr.expr
        return expectations

    @property
    def sdp_drop_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_sdp_compatible and e.action in ["DROP", "QUARANTINE"]:
                expectations[e.name] = e.expr.expr
        return expectations

    @property
    def sdp_fail_expectations(self) -> dict[str, str]:
        expectations = {}
        for e in self.expectations:
            if e.is_sdp_compatible and e.action == "FAIL":
                expectations[e.name] = e.expr.expr
        return expectations

    @computed_field(description="expectations_checkpoint_path")
    @property
    def expectations_checkpoint_path(self) -> Path | None:
        if self.expectations_checkpoint_path_:
            return Path(self.expectations_checkpoint_path_)

        if self.root_path:
            return Path(self.root_path) / "checkpoints/expectations"

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
        names = [
            src.node_name
            for src in self.sources
            if isinstance(src, PipelineNodeDataSource)
        ]
        names += self.depends_on
        if self.transformer:
            names += self.transformer.upstream_node_names
        return list(dict.fromkeys(names))  # deduplicate, preserve order

    # ----------------------------------------------------------------------- #
    # Data Sources                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def data_sources(self) -> list[BaseDataSource]:
        """Get all sources feeding the pipeline node (transformer-level inline sources only)."""
        sources = []
        if self.transformer:
            sources += self.transformer.data_sources
        return sources

    # ----------------------------------------------------------------------- #
    # Execution                                                               #
    # ----------------------------------------------------------------------- #

    def purge(self):
        logger.info(f"Purging pipeline node {self.name}")

        if self.has_sinks:
            for s in self.sinks:
                s.purge()

        if self.expectations_checkpoint_path:
            if os.path.exists(self.expectations_checkpoint_path):
                logger.info(
                    f"Deleting expectations checkpoint at {self.expectations_checkpoint_path}",
                )
                shutil.rmtree(self.expectations_checkpoint_path)

            # Try with DBFS
            # If spark is not used, dbfs is most likely not used
            if self.dataframe_backend != DataFrameBackends.PYSPARK:
                return

            # Check if a workspace client can be instantiated
            try:
                from databricks.sdk import WorkspaceClient
                from databricks.sdk.errors import ResourceDoesNotExist

                w = WorkspaceClient()

            except (
                ModuleNotFoundError,  # SDK not installed
                ImportError,  # SDK with different version / API
                ValueError,  # client not configure (would never happen in a notebook)
            ):
                return

            # Format path for DBFS
            _path = self.expectations_checkpoint_path.as_posix()
            if not _path.startswith("/") and not _path.startswith("dbfs:"):
                _path = "/" + _path
            if _path.startswith("/dbfs/"):
                _path = _path.replace("/dbfs/", "dbfs:/")
            if not _path.startswith("dbfs:"):
                _path = "dbfs:" + _path

            # Check Status
            try:
                w.dbfs.get_status(_path)
            except ResourceDoesNotExist:
                logger.info(
                    f"Expectation checkpoint at {_path} does not exist. Skipping.",
                )
                return

            logger.info(
                f"Deleting expectation checkpoint at {_path}.",
            )
            w.dbfs.delete(_path, recursive=True)

    def execute(
        self,
        apply_transformer: bool = True,
        write_sinks: bool = True,
        full_refresh: bool = False,
        named_dfs: dict[str, AnyFrame] = None,
        update_tables_metadata: bool = True,
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
        update_tables_metadata:
            Update tables metadata

        Returns
        -------
        :
            output Spark DataFrame
        """
        logger.info(f"Executing pipeline node {self.name}")

        # Install dependencies
        pl = self.parent_pipeline
        if pl and not pl._imports_imported:
            for package_name in pl._imports:
                try:
                    logger.info(f"Importing {package_name}")
                    importlib.import_module(package_name)
                except ModuleNotFoundError:
                    logger.info(f"Importing {package_name} failed.")
            pl._imports_imported = True

        # Skip sink writes when a declarative pipeline framework owns them
        if self.is_orchestrator_ldp or self.is_orchestrator_sdp:
            logger.info(
                "Declarative pipeline orchestrator selected. Sinks writing will be skipped."
            )
            write_sinks = False
            full_refresh = False

        # Refresh
        if full_refresh:
            self.purge()

        # Read all declared sources into named_dfs with "sources." prefix
        if named_dfs is None:
            named_dfs = {}
        for src in self.sources:
            src_key = src.name if src.name else "df"
            named_dfs[f"sources.{src_key}"] = src.read()

        # Primary df: always the first declared source of THIS node
        if self.sources:
            first_src_key = self.sources[0].name if self.sources[0].name else "df"
            self._stage_df = named_dfs.get(f"sources.{first_src_key}")
        else:
            self._stage_df = None

        # Pre-load upstream nodes referenced in transformer ({nodes.X} in SQL/method args)
        if apply_transformer and self.transformer and self.parent_pipeline:
            for upstream_name in self.transformer.upstream_node_names:
                key = f"nodes.{upstream_name}"
                if key not in named_dfs:
                    # Reuse if already loaded via a sources entry for the same node
                    existing = next(
                        (
                            df
                            for src, df in (
                                (
                                    src,
                                    named_dfs.get(
                                        f"sources.{src.name if src.name else 'df'}"
                                    ),
                                )
                                for src in self.sources
                                if isinstance(src, PipelineNodeDataSource)
                                and src.node_name == upstream_name
                            )
                            if df is not None
                        ),
                        None,
                    )
                    if existing is not None:
                        named_dfs[key] = existing
                    else:
                        tmp = PipelineNodeDataSource(node_name=upstream_name)
                        tmp._parent = self
                        named_dfs[key] = tmp.read()

        # Apply transformer
        if apply_transformer and self.transformer:
            self._stage_df = self.transformer.execute(
                self._stage_df, named_dfs=named_dfs
            )

        # Check expectations
        self._output_df = self._stage_df
        self._quarantine_df = None
        self.check_expectations()

        # Output and Quarantine to Sinks
        if write_sinks and self.sinks:
            view_definition = self.view_definition

            for s in self.sinks:
                # Get DataFrame
                _df = self._output_df
                if s.is_quarantine:
                    _df = self._quarantine_df

                # Create Sink
                s.create(df=_df)

                _is_update_metadata = (
                    update_tables_metadata and s.metadata and not self.is_ldp_execute
                )

                if self.is_view:
                    s.write(view_definition=view_definition)
                    if _is_update_metadata:
                        s.metadata.execute()
                    self._output_df = s.as_source().read()
                else:
                    if _is_update_metadata:
                        s.metadata.execute()
                    s.write(df=self._output_df)

                    # Metadata update required because of schema overwrite
                    if _is_update_metadata and s.metadata.update_required:
                        s.metadata.execute()

        return self._output_df

    def check_expectations(self):
        """
        Check expectations, raise errors, warnings where required and build
        filtered and quarantine DataFrames.

        Some actions have to be disabled when selected orchestrator is
        Databricks LDP:

        * Raising error on Failure when expectation is supported by LDP
        * Dropping rows when expectation is supported by LDP
        """

        # Data Quality Checks
        qfilter = None  # Quarantine filter
        kfilter = None  # Keep filter
        if self._stage_df is None:
            # Node without source or transformer
            return
        if not self.expectations:
            return
        if self.is_sdp_execute:
            # SDP blocks DataFrame analysis (AnalyzePlan RPCs) inside decorated
            # functions. SDP-compatible expectations are enforced via @dp.expect_*
            # decorators in laktory_sdp.py; Laktory cannot run checks in-process.
            names = [e.name for e in self.expectations if not e.is_sdp_compatible]
            if names:
                raise TypeError(
                    f"Expectations {names} are not natively supported by SDP and "
                    "cannot be enforced inside an SDP decorated function."
                )
            return
        is_streaming = getattr(nw.to_native(self._stage_df), "isStreaming", False)

        def _batch_check(df, node):
            for e in node.expectations:
                # Run Check: this only warn or raise exceptions.
                if not e.is_sdp_managed:
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

        logger.info("Checking Data Quality Expectations")

        if not is_streaming:
            _batch_check(
                self._stage_df,
                self,
            )

        else:
            skip = False

            if self.is_ldp_execute:
                names = []
                for e in self.expectations:
                    if not e.is_ldp_compatible:
                        names += [e.name]
                if names:
                    raise TypeError(
                        f"Expectations {names} are not natively supported by Lakeflow and can't be computed on a streaming DataFrame with Lakeflow executor."
                    )

                skip = True

            backend = DataFrameBackends.from_df(self._stage_df)
            if backend not in STREAMING_BACKENDS:
                raise TypeError(
                    f"DataFrame backend {backend} is not supported for streaming operations"
                )

            if self.expectations_checkpoint_path is None:
                raise ValueError(
                    f"Expectations Checkpoint not specified for node '{self.name}'"
                )

            # TODO: Refactor for backend other than spark
            if not skip:
                query = (
                    self._stage_df.to_native()
                    .writeStream.foreachBatch(
                        lambda batch_df, batch_id: _stream_check(
                            nw.from_native(batch_df), batch_id, self
                        )
                    )
                    .trigger(availableNow=True)
                    .options(
                        checkpointLocation=self.expectations_checkpoint_path,
                    )
                    .start()
                )
                query.awaitTermination()

        # Build Filters
        for e in self.expectations:
            # Update Keep Filter
            if not e.is_sdp_managed:
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
