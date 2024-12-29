from __future__ import annotations
from typing import Union
from typing import Literal
from typing import TYPE_CHECKING
from typing import Any
from pydantic import model_validator
from pathlib import Path
import networkx as nx

import laktory
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
from laktory.models.pipeline.pipelinenode import PipelineNode
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource

if TYPE_CHECKING:
    from plotly.graph_objs import Figure

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Classes                                                              #
# --------------------------------------------------------------------------- #


class PipelineUDF(BaseModel):
    """
    Pipeline User Define Function

    Attributes
    ----------
    module_name:
        Name of the module from which the function needs to be imported.
    function_name:
        Name of the function.
    module_path:
        Workspace filepath of the module, if not in the same directory as the pipeline notebook
    """

    module_name: str
    function_name: str
    module_path: str = None


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class Pipeline(BaseModel, PulumiResource, TerraformResource, PipelineChild):
    """
    Pipeline model to manage a full-fledged data pipeline including reading
    from data sources, applying data transformations through Spark and
    outputting to data sinks.

    A pipeline is composed of collections of `nodes`, each one defining its
    own source, transformations and optional sink. A node may be the source of
    another node.

    A pipeline may be run manually by using python or the CLI, but it may also
    be deployed and scheduled using one of the supported orchestrators, such as
    a Databricks Delta Live Tables or job.

    Attributes
    ----------
    databricks_job:
        Defines the Databricks Job specifications when DATABRICKS_JOB is
        selected as the orchestrator. Requires to add the supporting
        [notebook](https://github.com/okube-ai/laktory/blob/main/laktory/resources/quickstart-stacks/workflows/notebooks/jobs/job_laktory_pl.py)
        to the stack.
    databricks_dlt:
        Defines the Databricks DLT specifications when DATABRICKS_DLT is
        selected as the orchestrator. Requires to add the supporting
        [notebook](https://github.com/okube-ai/laktory/blob/main/laktory/resources/quickstart-stacks/workflows/notebooks/dlt/dlt_laktory_pl.py)
        to the stack.
    dependencies:
        List of dependencies required to run the pipeline. If Laktory is not
        provided, it's current version is added to the list.
    name:
        Name of the pipeline
    nodes:
        List of pipeline nodes. Each node defines a data source, a series
        of transformations and optionally a sink.
    orchestrator:
        Orchestrator used for scheduling and executing the pipeline. The
        selected option defines which resources are to be deployed.
        Supported options are:

        - `DATABRICKS_DLT`: When orchestrated through Databricks DLT, each
          pipeline node creates a DLT table (or view, if no sink is defined).
          Behind the scenes, `PipelineNodeDataSource` leverages native `dlt`
          `read` and `read_stream` functions to defined the interdependencies
          between the tables as in a standard DLT pipeline.
        - `DATABRICKS_JOB`: When deployed through a Databricks Job, a task
          is created for each pipeline node and all the required dependencies
          are set automatically. If a given task (or pipeline node) uses a
          `PipelineNodeDataSource` as the source, the data will be read from
          the upstream node sink.
    udfs:
        List of user defined functions provided to the transformer.
    root_path:
        Location of the pipeline node root used to store logs, metrics and
        checkpoints.

    Examples
    --------
    This first example shows how to configure a simple pipeline with 2 nodes.
    Upon execution, raw data will be read from a CSV file and two DataFrames
    (bronze and silver) will be created and saved as parquet files. Notice how
    the first node is used as a data source for the second node.

    ```py
    import io
    from laktory import models

    pipeline_yaml = '''
        name: pl-stock-prices
        nodes:
        - name: brz_stock_prices
          layer: BRONZE
          source:
            format: CSV
            path: ./raw/brz_stock_prices.csv
          sinks:
          - format: PARQUET
            mode: OVERWRITE
            path: ./dataframes/brz_stock_prices

        - name: slv_stock_prices
          layer: SILVER
          source:
            node_name: brz_stock_prices
          sinks:
          - format: PARQUET
            mode: OVERWRITE
            path: ./dataframes/slv_stock_prices
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
            - func_name: drop
              func_args:
              - value: data
              - value: producer
              - value: name
              - value: description
    '''

    pl = models.Pipeline.model_validate_yaml(io.StringIO(pipeline_yaml))

    # Execute pipeline
    # pl.execute()
    ```

    The next example defines a 3 nodes pipeline (1 bronze and 2 silvers)
    orchestrated with a Databricks Job. Notice how nodes are used as data
    sources not only for other nodes, but also for the `other` keyword argument
    of the smart join function (slv_stock_prices). Because we are using the
    DATABRICKS_JOB orchestrator, the job configuration must be declared.
    The tasks will be automatically created by the Pipeline model. Each
    task will execute a single node using the notebook referenced in
    `databricks_job.notebook_path` the content of this notebook should be
    similar to laktory.resources.notebooks.job_laktory.pl

    ```py
    import io
    from laktory import models

    pipeline_yaml = '''
        name: pl-stock-prices
        orchestrator: DATABRICKS_JOB
        databricks_job:
          name: job-pl-stock-prices
          notebook_path: /Workspace/.laktory/jobs/job_laktory_pl.py
          clusters:
            - name: node-cluster
              spark_version: 14.0.x-scala2.12
              node_type_id: Standard_DS3_v2
        dependencies:
            - laktory==0.3.0
            - yfinance
        nodes:
        - name: brz_stock_prices
          layer: BRONZE
          source:
            path: /Volumes/dev/sources/landing/events/yahoo-finance/stock_price/
          sinks:
          -   path: /Volumes/dev/sources/landing/tables/dev_stock_prices/
              mode: OVERWRITE

        - name: slv_stock_prices
          layer: SILVER
          source:
            node_name: brz_stock_prices
          sinks:
          -   path: /Volumes/dev/sources/landing/tables/slv_stock_prices/
              mode: OVERWRITE
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
            - func_name: drop
              func_args:
              - value: data
              - value: producer
              - value: name
              - value: description
            - func_name: laktory.smart_join
              func_kwargs:
                'on':
                  - symbol
                other:
                  node_name: slv_stock_meta

        - name: slv_stock_meta
          layer: SILVER
          source:
            path: /Volumes/dev/sources/landing/events/yahoo-finance/stock_meta/
          sinks:
          - path: /Volumes/dev/sources/landing/tables/slv_stock_meta/
            mode: OVERWRITE

    '''
    pl = models.Pipeline.model_validate_yaml(io.StringIO(pipeline_yaml))
    ```

    Finally, we re-implement the previous pipeline, but with a few key
    differences:

    - Orchestrator is `DATABRICKS_DLT` instead of a `DATABRICKS_JOB`
    - Sinks are Unity Catalog tables instead of storage locations
    - Data is read as a stream in most nodes
    - `slv_stock_meta` is simply a DLT view since it does not have an associated
      sink.

    We also need to provide some basic configuration for the DLT pipeline.

    ```py
    import io
    from laktory import models

    pipeline_yaml = '''
        name: pl-stock-prices
        orchestrator: DATABRICKS_DLT
        databricks_dlt:
            catalog: dev
            target: sandbox
            access_controls:
            - group_name: users
              permission_level: CAN_VIEW

        nodes:
        - name: brz_stock_prices
          layer: BRONZE
          source:
            path: /Volumes/dev/sources/landing/events/yahoo-finance/stock_price/
            as_stream: true
          sinks:
          - table_name: brz_stock_prices

        - name: slv_stock_prices
          layer: SILVER
          source:
            node_name: brz_stock_prices
            as_stream: true
          sinks:
          - table_name: slv_stock_prices
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
            - func_name: drop
              func_args:
              - value: data
              - value: producer
              - value: name
              - value: description
            - func_name: laktory.smart_join
              func_kwargs:
                'on':
                  - symbol
                other:
                  node_name: slv_stock_meta

        - name: slv_stock_meta
          layer: SILVER
          source:
            path: /Volumes/dev/sources/landing/events/yahoo-finance/stock_meta/

    '''
    pl = models.Pipeline.model_validate_yaml(io.StringIO(pipeline_yaml))
    ```

    References
    ----------
    * [Databricks Job](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html)
    * [Databricks DLT](https://www.databricks.com/product/delta-live-tables)

    """

    databricks_job: Union[DatabricksJobOrchestrator, None] = None
    databricks_dlt: Union[DatabricksDLTOrchestrator, None] = None
    dataframe_backend: Literal["SPARK", "POLARS"] = None
    dependencies: list[str] = []
    name: str
    nodes: list[Union[PipelineNode]]
    orchestrator: Union[Literal["DATABRICKS_DLT", "DATABRICKS_JOB"], None] = None
    udfs: list[PipelineUDF] = []
    root_path: str = None

    @model_validator(mode="before")
    @classmethod
    def assign_name(cls, data: Any) -> Any:

        if (
            "databricks_dlt" in data.keys()
            and data["databricks_dlt"].get("name", None) is None
        ):
            data["databricks_dlt"]["name"] = data.get("name", None)

        if (
            "databricks_job" in data.keys()
            and data["databricks_job"].get("name", None) is None
        ):
            data["databricks_job"]["name"] = data.get("name", None)

        return data

    @model_validator(mode="before")
    @classmethod
    def push_df_backend(cls, data: Any) -> Any:
        """Need to push dataframe_backend which is required to differentiate between spark and polars transformer"""
        df_backend = data.get("dataframe_backend", None)
        if df_backend:
            if "nodes" in data.keys():
                for n in data["nodes"]:
                    if isinstance(n, dict):
                        n["dataframe_backend"] = n.get("dataframe_backend", df_backend)
        return data

    @model_validator(mode="after")
    def validate_orchestrator(self):

        if self.orchestrator == "DATABRICKS_JOB":
            if self.databricks_job is None:
                raise ValueError(
                    "databricks_job must be defined if DATABRICKS_JOB orchestrator is selected."
                )
            self.databricks_job.update_from_parent()

        if self.orchestrator == "DATABRICKS_DLT":
            if self.databricks_dlt is None:
                raise ValueError(
                    "databricks_job must be defined if DATABRICKS_DLT orchestrator is selected."
                )
            self.databricks_dlt.update_from_parent()

        return self

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def child_attribute_names(self):
        return ["nodes", "databricks_dlt", "databricks_job"]

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

        laktory_found = False

        dependencies = [d for d in self.dependencies]
        for d in dependencies:
            if "laktory" in d:
                laktory_found = True

        if not laktory_found:
            dependencies += [f"laktory=={laktory.__version__}"]

        return dependencies

    # ----------------------------------------------------------------------- #
    # Orchestrator                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def is_orchestrator_dlt(self) -> bool:
        """If `True`, pipeline orchestrator is DLT"""
        return self.orchestrator == "DATABRICKS_DLT"

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
        self, spark=None, udfs=None, write_sinks=True, full_refresh: bool = False
    ) -> None:
        """
        Execute the pipeline (read sources and write sinks) by sequentially
        executing each node. The selected orchestrator might impact how
        data sources or sinks are processed.

        Parameters
        ----------
        spark:
            Spark Session
        udfs:
            List of user-defined functions used in transformation chains.
        write_sinks:
            If `False` writing of node sinks will be skipped
        full_refresh:
            If `True` all nodes will be completely re-processed by deleting
            existing data and checkpoints before processing.
        """
        logger.info("Executing Pipeline")

        for inode, node in enumerate(self.sorted_nodes):
            node.execute(
                spark=spark,
                udfs=udfs,
                write_sinks=write_sinks,
                full_refresh=full_refresh,
            )

    def dag_figure(self) -> Figure:
        """
        [UNDER DEVELOPMENT] Generate a figure representation of the pipeline
        DAG.

        Returns
        -------
        :
            Plotly figure representation of the pipeline.
        """
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

        if self.databricks_job:
            resources += [self.databricks_job]

        if self.databricks_dlt:
            resources += [self.databricks_dlt]

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
