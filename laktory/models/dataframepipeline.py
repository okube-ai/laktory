from typing import Union
from typing import Literal
from typing import TYPE_CHECKING
import networkx as nx

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.datasources.pipelinenodedatasource import PipelineNodeDataSource
from laktory.models.pipelinenode import PipelineNode
from laktory.models.databricks.pipeline import Pipeline

if TYPE_CHECKING:
    from plotly.graph_objs import Figure

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class DataFramePipeline(BaseModel):
    """

    Attributes
    ----------
    nodes:
        The list of pipeline nodes. Each node defines a data source, a series
        of transformations and optionally a sink.

    Examples
    --------
    ```py

    ```
    """

    dlt: Union[Pipeline, None] = None
    nodes: list[Union[PipelineNode]]
    engine: Union[Literal["DLT"], None] = "DLT"
    # orchestrator: Literal["DATABRICKS"] = "DATABRICKS"

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def nodes_dict(self) -> dict[str, PipelineNode]:
        return {n.id: n for n in self.nodes}

    @property
    def dag(self) -> nx.DiGraph:

        # TODO: Review if this needs to be computed dynamically or if we can
        #       compute it only after initialization. It depends if we believe
        #       that the nodes will be changed after initialization.

        dag = nx.DiGraph()

        # Build nodes
        for n in self.nodes:
            dag.add_node(n.id)

        # Build edges and assign nodes to pipeline node data sources
        for n in self.nodes:
            if isinstance(n.source, PipelineNodeDataSource):
                dag.add_edge(n.source.node_id, n.id)
                n.source.node = self.nodes_dict[n.source.node_id]

            if not n.chain:
                continue

            for sn in n.chain.nodes:
                for a in sn.spark_func_args:
                    if isinstance(a.value, PipelineNodeDataSource):
                        dag.add_edge(a.value.node_id, n.id)
                        a.value.node = self.nodes_dict[a.value.node_id]
                for a in sn.spark_func_kwargs.values():
                    if isinstance(a.value, PipelineNodeDataSource):
                        dag.add_edge(a.value.node_id, n.id)
                        a.value.node = n
                        a.value.node = self.nodes_dict[a.value.node_id]

        if not nx.is_directed_acyclic_graph(dag):
            raise ValueError(
                "Pipeline is not a DAG (directed acyclic graph)."
                " A circular dependency has been detected. Please review nodes dependencies."
            )

        return dag

    @property
    def sorted_node_ids(self):
        return nx.topological_sort(self.dag)

    @property
    def sorted_nodes(self):
        node_ids = self.sorted_node_ids
        nodes = []
        for node_id in node_ids:
            for node in self.nodes:
                if node.id == node_id:
                    nodes += [node]
                    break

        return nodes

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def execute(self, spark, udfs=None) -> None:
        logger.info("Executing Pipeline")

        for inode, node in enumerate(self.sorted_nodes):
            logger.info(f"Executing node {inode} ({node.id}).")
            node.execute(spark, udfs=udfs)

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
