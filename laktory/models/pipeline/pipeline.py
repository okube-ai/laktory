from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import networkx as nx
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.dataquality.check import DataQualityCheck
from laktory.models.pipeline.orchestrators.databricksdltorchestrator import (
    DatabricksDLTOrchestrator,
)
from laktory.models.pipeline.orchestrators.databricksjoborchestrator import (
    DatabricksJobOrchestrator,
)
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.models.pipeline.pipelinenode import PipelineNode
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.typing import AnyFrame

if TYPE_CHECKING:
    from plotly.graph_objs import Figure

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Classes                                                              #
# --------------------------------------------------------------------------- #


# class PipelineUDF(BaseModel):
#     # TODO: Revisit to allow for automatic registration of UDF
#     """
#     Pipeline User Define Function
#
#     Parameters
#     ----------
#     module_name:
#         Name of the module from which the function needs to be imported.
#     function_name:
#         Name of the function.
#     module_path:
#         Workspace filepath of the module, if not in the same directory as the pipeline notebook
#     """
#
#     module_name: str
#     function_name: str
#     module_path: str = None


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class Pipeline(BaseModel, PulumiResource, TerraformResource, PipelineChild):
    """
    Pipeline model to manage a data pipeline including reading from data sources,
    applying data transformations and outputting to data sinks.

    A pipeline is composed of collections of `nodes`, each one defining its
    own source, transformations and optional sink. A node may be the source of
    another node.

    A pipeline may be run manually by using python or the CLI, but it may also
    be deployed and scheduled using one of the supported orchestrators, such as
    a Databricks Delta Live Tables or job.

    The DataFrame backend used to run the pipeline can be configured at the pipeline
    level or at the nodes level.

    Examples
    --------
    This first example shows how to configure a simple pipeline with 2 nodes.
    Upon execution, raw data will be read from a JSON files and two DataFrames
    (bronze and silver) will be created and saved as parquet files. Notice how
    the first node is used as a data source for the second node. Polars is used
    as the DataFrame backend.

    ```py
    import io

    import laktory as lk

    pipeline_yaml = '''
        name: pl-stock-prices
        dataframe_backend: POLARS

        nodes:
        - name: brz_stock_prices
          source:
            path: ./data/stock_prices/
            format: JSONL
          sinks:
          - path: ./data/brz_stock_prices.parquet
            format: PARQUET

        - name: slv_stock_prices
          source:
            node_name: brz_stock_prices
            as_stream: false
          sinks:
          - path: ./data/slv_stock_prices.parquet
            format: PARQUET
          transformer:
            nodes:
            - expr: |
                SELECT
                  CAST(data.created_at AS TIMESTAMP) AS created_at,
                  data.symbol AS name,
                  data.symbol AS symbol,
                  data.open AS open,
                  data.close AS close,
                  data.high AS high,
                  data.low AS low,
                  data.volume AS volume
                FROM
                  {df}
            - func_name: unique
              func_kwargs:
                subset:
                  - symbol
                  - created_at
                keep:
                  any
    '''

    pl = lk.models.Pipeline.model_validate_yaml(io.StringIO(pipeline_yaml))

    # Execute pipeline
    # pl.execute()
    ```

    The next example also defines a 2 nodes pipeline, but uses PySpark as the DataFrame
    backend. It defines the configuration required to deploy it as a Databricks job. In
    this case, the sinks are writing to unity catalog tables.

    ```py
    import io

    import laktory as lk

    pipeline_yaml = '''
        name: pl-stocks-job
        dataframe_backend: PYSPARK
        orchestrator:
          type: DATABRICKS_JOB

        nodes:

        - name: brz_stock_prices
          source:
            path: dbfs:/laktory/data/stock_prices/
            as_stream: false
            format: JSONL
          sinks:
          - table_name: brz_stock_prices_job
            mode: OVERWRITE

        - name: slv_stock_prices
          expectations:
          - name: positive_price
            expr: open > 0
            action: DROP
          source:
            node_name: brz_stock_prices
            as_stream: false
          sinks:
          - table_name: slv_stock_prices_job
            mode: OVERWRITE

          transformer:
            nodes:
            - expr: |
                SELECT
                    cast(data.created_at AS TIMESTAMP) AS created_at,
                    data.symbol AS symbol,
                    data.open AS open,
                    data.close AS close
                FROM
                    {df}
            - func_name: drop_duplicates
              func_kwargs:
                subset: ["created_at", "symbol"]
              dataframe_api: NATIVE
    '''
    pl = lk.models.Pipeline.model_validate_yaml(io.StringIO(pipeline_yaml))
    ```
    """

    dependencies: list[str] = Field(
        [],
        description="List of dependencies required to run the pipeline. If Laktory is not provided, it's current version is added to the list.",
    )
    name: str = Field(..., description="Name of the pipeline")
    nodes: list[PipelineNode] = Field(
        [],
        description="List of pipeline nodes. Each node defines a data source, a series of transformations and optionally a sink.",
    )
    orchestrator: DatabricksJobOrchestrator | DatabricksDLTOrchestrator = Field(
        None,
        description="""
        Orchestrator used for scheduling and executing the pipeline. The
        selected option defines which resources are to be deployed.
        Supported options are instances of classes:

        - `DatabricksJobOrchestrator`: When orchestrated through Databricks DLT, each
          pipeline node creates a DLT table (or view, if no sink is defined).
          Behind the scenes, `PipelineNodeDataSource` leverages native `dlt`
          `read` and `read_stream` functions to defined the interdependencies
          between the tables as in a standard DLT pipeline.
        - `DatabricksDLTOrchestrator`: When deployed through a Databricks Job, a task
          is created for each pipeline node and all the required dependencies
          are set automatically. If a given task (or pipeline node) uses a
          `PipelineNodeDataSource` as the source, the data will be read from
          the upstream node sink.
        """,
        # discriminator="type",  # discriminator can't be used because BaseModel adds
        # str to Literal type to support variables
    )
    root_path: str = Field(
        None,
        description="Location of the pipeline node root used to store logs, metrics and checkpoints.",
    )

    @field_validator("root_path", mode="before")
    @classmethod
    def root_path_to_string(cls, value: Any) -> Any:
        if isinstance(value, Path):
            value = str(value)
        return value

    @model_validator(mode="before")
    @classmethod
    def assign_name(cls, data: Any) -> Any:
        o = data.get("orchestrator", None)
        if o and isinstance(o, dict):
            # orchestrator as a dict
            o["name"] = o.get("name", None) or data.get("name", None)
        elif isinstance(o, (DatabricksDLTOrchestrator, DatabricksJobOrchestrator)):
            # orchestrator as a model
            o.name = o.name or o.get("name", None)

        return data

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return ["nodes", "orchestrator"]

    # ----------------------------------------------------------------------- #
    # ID                                                                      #
    # ----------------------------------------------------------------------- #

    @property
    def resolved_name(self) -> str:
        return self.inject_vars_into_dump({"name": self.name})["name"]

    @property
    def safe_name(self):
        name = self.resolved_name

        # Replace special characters
        chars = [" ", ".", "@", "{", "}", "[", "]", "$", "|"]
        for c in chars:
            name = name.replace(c, "-")

        return name

    # ----------------------------------------------------------------------- #
    # Libraries                                                               #
    # ----------------------------------------------------------------------- #

    @property
    def _dependencies(self):
        from laktory import __version__

        laktory_found = False

        dependencies = [d for d in self.dependencies]
        for d in dependencies:
            if "laktory" in d:
                laktory_found = True

        if not laktory_found:
            dependencies += [f"laktory=={__version__}"]

        return dependencies

    # ----------------------------------------------------------------------- #
    # Orchestrator                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def is_orchestrator_dlt(self) -> bool:
        """If `True`, pipeline orchestrator is DLT"""
        return isinstance(self.orchestrator, DatabricksDLTOrchestrator)

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def _root_path(self) -> Path:
        if self.root_path:
            return Path(self.root_path)

        return Path(settings.laktory_root) / "pipelines" / self.safe_name

    # ----------------------------------------------------------------------- #
    # Expectations                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def checks(self) -> list[DataQualityCheck]:
        checks = []
        for node in self.nodes:
            checks += node.checks
        return checks

    # ----------------------------------------------------------------------- #
    # Nodes                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def nodes_dict(self) -> dict[str, PipelineNode]:
        """
        Nodes dictionary whose keys are the node names.

        Returns
        -------
        :
            Nodes
        """
        return {n.name: n for n in self.nodes}

    @property
    def dag(self) -> nx.DiGraph:
        """
        Networkx Directed Acyclic Graph representation of the pipeline. Useful
        to identify interdependencies between nodes.

        Returns
        -------
        :
            Directed Acyclic Graph
        """
        import networkx as nx

        # TODO: Review if this needs to be computed dynamically or if we can
        #       compute it only after initialization. It depends if we believe
        #       that the nodes will be changed after initialization.

        dag = nx.DiGraph()

        # Build nodes
        for n in self.nodes:
            dag.add_node(n.name)
        # Build edges and assign nodes to pipeline node data sources
        node_names = []
        for n in self.nodes:
            if n.name in node_names:
                raise ValueError(
                    f"Pipeline node '{n.name}' is declared twice in pipeline '{self.name}'"
                )
            node_names += [n.name]

            # for s in n.get_sources(PipelineNodeDataSource):
            for _node_name in n.upstream_node_names:
                dag.add_edge(_node_name, n.name)
                if _node_name not in self.nodes_dict:
                    raise ValueError(
                        f"Pipeline node data source '{_node_name}' is not defined in pipeline '{self.name}'"
                    )

        if not nx.is_directed_acyclic_graph(dag):
            for n in dag.nodes:
                logger.info(f"Pipeline {self.name} node: {n}")
            for e in dag.edges:
                logger.info(f"Pipeline {self.name} edge: {e[0]} -> {e[1]}")
            raise ValueError(
                f"Pipeline '{self.name}' is not a DAG (directed acyclic graph)."
                " A circular dependency has been detected. Please review nodes dependencies."
            )

        return dag

    @property
    def sorted_node_names(self):
        import networkx as nx

        return list(nx.topological_sort(self.dag))

    @property
    def sorted_nodes(self) -> list[PipelineNode]:
        """
        Topologically sorted nodes.

        Returns
        -------
        :
            List of Topologically sorted nodes.
        """
        node_names = self.sorted_node_names
        nodes = []
        for node_name in node_names:
            for node in self.nodes:
                if node.name == node_name:
                    nodes += [node]
                    break

        return nodes

    # ----------------------------------------------------------------------- #
    # Data Sources                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def data_sources(self):
        sources = []
        for n in self.nodes:
            sources += n.data_sources
        return sources

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def purge(self, spark=None) -> None:
        logger.info("Purging Pipeline")

        for inode, node in enumerate(self.sorted_nodes):
            node.purge(
                spark=spark,
            )

    def execute(
        self,
        write_sinks=True,
        full_refresh: bool = False,
        named_dfs: dict[str, AnyFrame] = None,
    ) -> None:
        """
        Execute the pipeline (read sources and write sinks) by sequentially
        executing each node. The selected orchestrator might impact how
        data sources or sinks are processed.

        Parameters
        ----------
        write_sinks:
            If `False` writing of node sinks will be skipped
        full_refresh:
            If `True` all nodes will be completely re-processed by deleting
            existing data and checkpoints before processing.
        named_dfs:
            Named DataFrames to be passed to pipeline nodes transformer.
        """
        logger.info("Executing Pipeline")

        for inode, node in enumerate(self.sorted_nodes):
            if named_dfs is None:
                named_dfs = {}

            node.execute(
                write_sinks=write_sinks,
                full_refresh=full_refresh,
                named_dfs=named_dfs,
            )

    def dag_figure(self) -> "Figure":
        """
        [UNDER DEVELOPMENT] Generate a figure representation of the pipeline
        DAG.

        Returns
        -------
        :
            Plotly figure representation of the pipeline.
        """
        import networkx as nx
        import plotly.graph_objs as go

        dag = self.dag
        pos = nx.spring_layout(dag)

        # ------------------------------------------------------------------- #
        # Edges                                                               #
        # ------------------------------------------------------------------- #

        edge_traces = []
        for e in dag.edges:
            edge_traces += [
                go.Scatter(
                    x=[pos[e[0]][0], pos[e[1]][0]],
                    y=[pos[e[0]][1], pos[e[1]][1]],
                    line={
                        "color": "#a006b1",
                    },
                    marker=dict(
                        symbol="arrow-bar-up",
                        angleref="previous",
                        size=30,
                    ),
                    mode="lines+markers",
                    hoverinfo="none",
                    showlegend=False,
                )
            ]

        # ------------------------------------------------------------------- #
        # Nodes                                                               #
        # ------------------------------------------------------------------- #

        nodes_trace = go.Scatter(
            x=[_p[0] for _p in pos.values()],
            y=[_p[1] for _p in pos.values()],
            text=list(dag.nodes),
            name="pl nodes",
            mode="markers+text",
            marker=dict(
                size=50,
                color="#06d49e",
                line=dict(
                    width=2,
                    color="#dff2ed",
                ),
            ),
            textfont=dict(
                color="#a006b1",
            ),
        )

        return go.Figure(data=[nodes_trace] + edge_traces)

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def self_as_core_resources(self):
        return False

    @property
    def resource_type_id(self) -> str:
        """
        pl
        """
        return "pl"

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        if orchestrator is `DLT`:

        - DLT Pipeline

        if orchestrator is `DATABRICKS_JOB`:

        - Databricks Job
        """

        resources = []

        if self.orchestrator:
            resources += [self.orchestrator]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return ""

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return ""
