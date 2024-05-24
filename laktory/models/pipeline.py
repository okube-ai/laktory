import os
import json
from typing import Union
from typing import Literal
from typing import TYPE_CHECKING
from typing import Any
from pydantic import model_validator
import networkx as nx

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.constants import CACHE_ROOT
from laktory.models.basemodel import BaseModel
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


class LaktoryPipelineJob(Job):
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


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class Pipeline(BaseModel, PulumiResource, TerraformResource):
    """
    Pipeline model to manage a full-fledged data pipeline including reading
    from data sources, applying data transformations through Spark and output
    to data sinks.

    A pipeline is composed of collections of `nodes`, each one defining its
    own source, transformations and sink. A node may be the source of another
    node.

    Multiple engines are supported for running the pipeline ... [TODO: Complete]

    Multiple orchestrators are supported for running the pipeline ... [TODO: Complete]

    Attributes
    ----------
    dlt:
        Delta Live Tables specifications if DLT engine is selected.
    nodes:
        The list of pipeline nodes. Each node defines a data source, a series
        of transformations and optionally a sink.
    engine:
        Selected engine for execution. TODO: Complete

    Examples
    --------
    ```py
    # TODO
    ```
    """

    databricks_job: Union[LaktoryPipelineJob, None] = None
    dlt: Union[DLTPipeline, None] = None
    name: str
    nodes: list[Union[PipelineNode]]
    engine: Union[Literal["DLT", "DATABRICKS_JOB"], None] = None
    udfs: list[PipelineUDF] = []

    @model_validator(mode="before")
    @classmethod
    def assign_to_dlt(cls, data: Any) -> Any:
        if "dlt" in data.keys():
            data["dlt"]["name"] = data.get("name", None)
        return data

    @model_validator(mode="after")
    def update_nodes(self) -> Any:
        """ """

        # Build dag
        _ = self.dag

        for n in self.nodes:
            # Assign pipeline
            n._pipeline = self

        return self

    @model_validator(mode="after")
    def validate_dlt(self) -> Any:

        if not self.is_engine_dlt:
            return self

        if self.dlt is None:
            raise ValueError("dlt must be defined if DLT engine is selected.")

        for n in self.nodes:

            # Sink must be Table or None
            if n.sink is None:
                pass
            if isinstance(n.sink, TableDataSink):
                if n.sink.table_name != n.name:
                    raise ValueError(
                        "For DLT pipeline, table sink name must be the same as node id."
                    )
                n.sink.catalog_name = self.dlt.catalog
                n.sink.schema_name = self.dlt.target

        return self

    @model_validator(mode="after")
    def validate_job(self) -> Any:

        if not self.engine == "DATABRICKS_JOB":
            return self

        if self.databricks_job is None:
            raise ValueError(
                "databricks_job must be defined if DATABRICKS_JOB engine is selected."
            )

        cluster_found = False
        for c in self.databricks_job.clusters:
            if c.name == "node-cluster":
                cluster_found = True
        if not cluster_found:
            raise ValueError(
                "To use DATABRICKS_JOB engine, a cluster named `node-cluster` must be defined in the databricks_job attribute."
            )

        job = self.databricks_job
        job.parameters = [JobParameter(name="pipeline_name", default=self.name)]

        notebook_path = self.databricks_job.notebook_path
        if notebook_path is None:
            notebook_path = f"{settings.workspace_laktory_root}jobs/job_laktory_pl.py"

        package = "laktory"
        if self.databricks_job.laktory_version:
            package += f"=={self.databricks_job.laktory_version}"

        job.tasks = []
        for node in self.sorted_nodes:

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
                    libraries=[{"pypi": {"package": package}}],
                    depends_ons=depends_on,
                    job_cluster_key="node-cluster",
                )
            ]

        return self

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def is_engine_dlt(self) -> bool:
        """If `True`, pipeline engine is DLT"""
        return self.engine == "DLT"

    @property
    def nodes_dict(self) -> dict[str, PipelineNode]:
        return {n.name: n for n in self.nodes}

    @property
    def dag(self) -> nx.DiGraph:

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
    def sorted_nodes(self):
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

    def execute(self, spark, udfs=None) -> None:
        logger.info("Executing Pipeline")

        for inode, node in enumerate(self.sorted_nodes):
            node.execute(spark=spark, udfs=udfs)

    def dag_figure(self) -> "Figure":

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
        """Flag set to `True` if self must be included in core resources"""
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
        if engine is DLT:
        - DLT Pipeline
        """
        # Configuration file
        source = os.path.join(CACHE_ROOT, f"tmp-{self.name}.json")
        d = self.model_dump(exclude_none=True)
        d = self.inject_vars(d)
        s = json.dumps(d, indent=4)
        with open(source, "w", newline="\n") as fp:
            fp.write(s)
        filepath = f"{settings.workspace_laktory_root}pipelines/{self.name}.json"
        file = WorkspaceFile(
            path=filepath,
            source=source,
        )

        resources = [file]

        resources += [
            Permissions(
                resource_name=f"permissions-{file.resource_name}",
                access_controls=[
                    AccessControl(
                        permission_level="CAN_READ",
                        group_name="account users",
                    )
                ],
                workspace_file_path=filepath,
                options={"depends_on": [f"${{resources.{file.resource_name}}}"]},
            )
        ]

        if self.is_engine_dlt:
            resources += [self.dlt]

        if self.engine == "DATABRICKS_JOB":
            resources += [self.databricks_job]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return ""  # "databricks:Pipeline"

    @property
    def pulumi_cls(self):
        return None

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
