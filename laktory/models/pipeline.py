from __future__ import annotations
import os
import json
from typing import Union
from typing import Literal
from typing import TYPE_CHECKING
from typing import Any
from pydantic import model_validator
from pathlib import Path
from pydantic import Field
import networkx as nx

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.constants import CACHE_ROOT
from laktory.constants import DEFAULT_DFTYPE
from laktory.models.basemodel import BaseModel
from laktory.models.dataquality.check import DataQualityCheck
from laktory.models.datasources.pipelinenodedatasource import PipelineNodeDataSource
from laktory.models.datasinks.tabledatasink import TableDataSink
from laktory.models.pipelinenode import PipelineNode
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.dltpipeline import DLTPipeline
from laktory.models.resources.databricks.job import Job
from laktory.models.resources.databricks.job import JobTask
from laktory.models.resources.databricks.job import JobParameter
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.databricks.workspacefile import WorkspaceFile
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


class PipelineDatabricksJob(Job):
    """
    Databricks job specifically designed to run a Laktory pipeline

    Attributes
    ----------
    laktory_version:
        Laktory version to use in the notebook tasks
    notebook_path:
        Path for the notebook. If `None`, default path for laktory job notebooks is used.

    """

    laktory_version: Union[str, None] = None
    notebook_path: Union[str, None] = None

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return super().pulumi_excludes + ["laktory_version", "notebook_path"]


class PipelineWorkspaceFile(WorkspaceFile):
    """
    Workspace File with default value for path and access controls and forced
    value for source given a pipeline name.
    """

    pipeline_name: str

    @model_validator(mode="before")
    @classmethod
    def default_values(cls, data: Any) -> Any:
        pl_name = data.get("pipeline_name", None)
        data["source"] = os.path.join(CACHE_ROOT, f"tmp-{pl_name}.json")
        if "path" not in data:
            data["path"] = f"{settings.workspace_laktory_root}pipelines/{pl_name}.json"
        if "access_controls" not in data:
            data["access_controls"] = [
                {"permission_level": "CAN_READ", "group_name": "users"}
            ]
        return data

    @property
    def resource_type_id(self):
        return "workspace-file"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return super().pulumi_excludes + ["pipeline_name"]


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class Pipeline(BaseModel, PulumiResource, TerraformResource):
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
        selected as the orchestrator.
    dlt:
        Defines the Delta Live Tables specifications when DLT is selected as
        the orchestrator.
    name:
        Name of the pipeline
    nodes:
        List of pipeline nodes. Each node defines a data source, a series
        of transformations and optionally a sink.
    orchestrator:
        Orchestrator used for scheduling and executing the pipeline. The
        selected option defines which resources are to be deployed.
        Supported options are:
        - `DLT`: When orchestrated through Databricks DLT, each pipeline node
          creates a DLT table (or view, if no sink is defined). Behind the
          scenes, `PipelineNodeDataSource` leverages native `dlt` `read` and
          `read_stream` functions to defined the interdependencies between the
          tables as in a standard DLT pipeline. This is the recommended
          orchestrator as it is the most feature rich.
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
    workspacefile:
        Workspace file used to store the JSON definition of the pipeline.


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
          laktory_version: 0.3.0
          notebook_path: /Workspace/.laktory/jobs/job_laktory_pl.py
          clusters:
            - name: node-cluster
              spark_version: 14.0.x-scala2.12
              node_type_id: Standard_DS3_v2

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

    - Orchestrator is `DLT` instead of a `DATABRICKS_JOB`
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
        orchestrator: DLT
        dlt:
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

    databricks_job: Union[PipelineDatabricksJob, None] = None
    dataframe_type: Literal["SPARK", "POLARS"] = DEFAULT_DFTYPE
    dlt: Union[DLTPipeline, None] = None
    name: str
    nodes: list[Union[PipelineNode]]
    orchestrator: Union[Literal["DLT", "DATABRICKS_JOB"], None] = None
    udfs: list[PipelineUDF] = []
    root_path: str = None
    workspacefile: PipelineWorkspaceFile = None

    @model_validator(mode="before")
    @classmethod
    def assign_name_to_dlt(cls, data: Any) -> Any:

        if "dlt" in data.keys():
            data["dlt"]["name"] = data.get("name", None)

        return data

    @model_validator(mode="before")
    @classmethod
    def assign_name_to_workspacefile(cls, data: Any) -> Any:
        workspacefile = data.get("workspacefile", None)
        if workspacefile:
            workspacefile["pipeline_name"] = data["name"]
        return data

    @model_validator(mode="before")
    @classmethod
    def push_dftype_before(cls, data: Any) -> Any:
        dftype = data.get("dataframe_type", None)
        if dftype:
            if "nodes" in data.keys():
                for n in data["nodes"]:
                    if isinstance(n, dict):
                        n["dataframe_type"] = n.get("dataframe_type", dftype)
        return data

    @model_validator(mode="after")
    def push_dftype_after(self) -> Any:
        dftype = self.user_dftype
        if dftype:
            for node in self.nodes:
                node.dataframe_type = node.user_dftype or dftype
                node.push_dftype_after()
        return self

    @model_validator(mode="after")
    def update_children(self) -> Any:
        # Build dag
        _ = self.dag

        # Assign pipeline
        for n in self.nodes:
            n._parent = self

        return self

    @model_validator(mode="after")
    def validate_dlt(self) -> Any:

        if not self.is_orchestrator_dlt:
            return self

        if self.dlt is None:
            raise ValueError("dlt must be defined if DLT orchestrator is selected.")

        for n in self.nodes:
            for s in n.all_sinks:
                if isinstance(s, TableDataSink):
                    s.catalog_name = self.dlt.catalog
                    s.schema_name = self.dlt.target

        return self

    @model_validator(mode="after")
    def validate_job(self) -> Any:

        if not self.orchestrator == "DATABRICKS_JOB":
            return self

        if self.databricks_job is None:
            raise ValueError(
                "databricks_job must be defined if DATABRICKS_JOB orchestrator is selected."
            )

        cluster_found = False
        for c in self.databricks_job.clusters:
            if c.name == "node-cluster":
                cluster_found = True
        # if not cluster_found:
        #     raise ValueError(
        #         "To use DATABRICKS_JOB orchestrator, a cluster named `node-cluster` must be defined in the databricks_job attribute."
        #     )

        job = self.databricks_job
        job.parameters = [
            JobParameter(name="pipeline_name", default=self.name),
            JobParameter(name="full_refresh", default="false"),
        ]

        notebook_path = self.databricks_job.notebook_path
        if notebook_path is None:
            notebook_path = f"{settings.workspace_laktory_root}jobs/job_laktory_pl.py"

        package = "laktory"
        if self.databricks_job.laktory_version:
            package += f"=={self.databricks_job.laktory_version}"

        job.tasks = []

        # Sorting Node Names to prevent job update trigger with Pulumi
        node_names = [node.name for node in self.nodes]
        node_names.sort()
        for node_name in node_names:

            node = self.nodes_dict[node_name]

            depends_on = []
            for edge in self.dag.in_edges(node.name):
                depends_on += [{"task_key": "node-" + edge[0]}]

            job.tasks += [
                JobTask(
                    task_key="node-" + node.name,
                    notebook_task={
                        "base_parameters": {"node_name": node.name},
                        "notebook_path": notebook_path,
                    },
                    # libraries=[{"pypi": {"package": package}}],
                    depends_ons=depends_on,
                    # job_cluster_key=job_cluster_key,
                )
            ]
            job.sort_tasks(job.tasks)

            if cluster_found:
                job.tasks[-1].job_cluster_key = "node-cluster"

        return self

    # ----------------------------------------------------------------------- #
    # DataFrame                                                               #
    # ----------------------------------------------------------------------- #

    @property
    def user_dftype(self):
        if "dataframe_type" in self.model_fields_set:
            return self.dataframe_type
        return None

    # ----------------------------------------------------------------------- #
    # Orchestrator                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def is_orchestrator_dlt(self) -> bool:
        """If `True`, pipeline orchestrator is DLT"""
        return self.orchestrator == "DLT"

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def _root_path(self) -> Path:
        if self.root_path:
            return Path(self.root_path)

        return Path(settings.laktory_root) / "pipelines" / self.name

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
        for n in self.nodes:
            for s in n.get_sources(PipelineNodeDataSource):
                dag.add_edge(s.node_name, n.name)
                if s.node_name not in self.nodes_dict:
                    raise ValueError(
                        f"Pipeline node data source '{s.node_name}' is not defined in pipeline '{self.name}'"
                    )
                s.node = self.nodes_dict[s.node_name]

        if not nx.is_directed_acyclic_graph(dag):
            raise ValueError(
                "Pipeline is not a DAG (directed acyclic graph)."
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
        - configuration workspace file
        - configuration workspace file permissions

        if orchestrator is `DLT`:

        - DLT Pipeline

        if orchestrator is `DATABRICKS_JOB`:

        - Databricks Job
        """
        # Configuration file
        source = os.path.join(CACHE_ROOT, f"tmp-{self.name}.json")
        d = self.model_dump(exclude_unset=True)
        d = self.inject_vars(d)
        s = json.dumps(d, indent=4)
        with open(source, "w", newline="\n") as fp:
            fp.write(s)

        resources = []
        if self.orchestrator in ["DATABRICKS_JOB", "DLT"]:

            file = self.workspacefile
            if file is None:
                file = PipelineWorkspaceFile(pipeline_name=self.name)
            resources += [file]

            if self.is_orchestrator_dlt:
                resources += [self.dlt]

            if self.orchestrator == "DATABRICKS_JOB":
                resources += [self.databricks_job]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return ""  # "databricks:Pipeline"

    #
    # @property
    # def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
    #     return {
    #         "access_controls": True,
    #         "tables": True,
    #         "clusters": {"__all__": {"access_controls"}},
    #         "udfs": True,
    #     }
    #
    # @property
    # def pulumi_properties(self):
    #     d = super().pulumi_properties
    #     k = "clusters"
    #     if k in d:
    #         _clusters = []
    #         for c in d[k]:
    #             c["label"] = c.pop("name")
    #             _clusters += [c]
    #         d[k] = _clusters
    #     return d

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return ""

    #
    # @property
    # def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
    #     return self.pulumi_excludes
    #
    # @property
    # def terraform_properties(self) -> dict:
    #     d = super().terraform_properties
    #     k = "cluster"
    #     if k in d:
    #         _clusters = []
    #         for c in d[k]:
    #             c["label"] = c.pop("name")
    #             _clusters += [c]
    #         d[k] = _clusters
    #     return d
