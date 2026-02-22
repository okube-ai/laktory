from collections import defaultdict

import networkx as nx
from pydantic import Field
from pydantic import SkipValidation

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.pipeline._execute import _execute  # noqa: F401
from laktory.models.pipeline._post_execute import _post_execute  # noqa: F401
from laktory.models.pipeline.pipeline import Pipeline
from laktory.models.pipeline.pipelinetask import PipelineTask

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Functions                                                            #
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class PipelineExecutionPlan(BaseModel):
    """
    A pipeline execution plan defines the pipeline tasks to be executed according to the selected nodes/groups/tags
    and their dependencies. It constructs a DAG of pipeline tasks, where each task can consist of one or more nodes
    that share the same group.
    """

    pipeline: SkipValidation[Pipeline] = Field(
        ...,
        description="""Pipeline""",
    )
    selects: list[str] | None = Field(
        None,
        description="""
        List of node names with optional dependency notation:
        
        - `{node_name}`: Execute the node only.
        - `*{node_name}`: Execute the node and its upstream dependencies.
        - `{node_name}*`: Execute the node and its downstream dependencies.
        - `*{node_name}*`: Execute the node, its upstream, and downstream dependencies.
        """,
    )

    @property
    def dag(self):
        """
        Constructs a DAG for the execution plan, where each node is either:
        - A single pipeline node (if it does not belong to a group), or
        - A group of nodes (if they share the same group).

        Returns
        -------
        nx.DiGraph
            A directed acyclic graph representing the execution plan.
        """
        pipeline_dag = self.pipeline.dag
        selected_nodes = set(self.node_names)

        # Create a mapping of groups to their nodes
        group_mapping = defaultdict(lambda: [])
        for node in self.pipeline.sorted_nodes:
            if node.name in selected_nodes:
                if node.group:
                    group_mapping[f"group-{node.group}"] += [node.name]
                else:
                    group_mapping[f"node-{node.name}"] += [node.name]

        # Initialize a new DAG
        execution_dag = nx.DiGraph()

        # Add nodes (individual nodes or groups) to the execution DAG
        for group, nodes in group_mapping.items():
            execution_dag.add_node(group, node_names=nodes)

        # Add edges based on the pipeline DAG
        for upstream, downstream in pipeline_dag.edges:
            # Find the groups or nodes for upstream and downstream
            upstream_group = next(
                (group for group, nodes in group_mapping.items() if upstream in nodes),
                None,
            )
            downstream_group = next(
                (
                    group
                    for group, nodes in group_mapping.items()
                    if downstream in nodes
                ),
                None,
            )

            if (
                upstream_group
                and downstream_group
                and upstream_group != downstream_group
            ):
                execution_dag.add_edge(upstream_group, downstream_group)

        return execution_dag

    @property
    def tasks(self):
        """
        List of pipeline tasks to be executed.

        Returns
        -------
        output: list[PipelineTask]
        """
        tasks = []
        for dag_node, attr in self.dag.nodes(data=True):
            tasks += [
                PipelineTask(
                    name=dag_node,
                    pipeline=self.pipeline,
                    node_names=attr["node_names"],
                )
            ]

        return tasks

    @property
    def tasks_dict(self):
        """Tasks dictionary"""
        return {task.name: task for task in self.tasks}

    @property
    def node_names(self) -> list[str]:
        if self.selects is None:
            return self.pipeline.sorted_node_names

        # Validate and resolve selected nodes/groups/tags
        selected_nodes = set()
        dag = self.pipeline.dag

        for item in self.selects:
            if item.startswith("*") and item.endswith("*"):
                # Node + both upstream and downstream dependencies
                node_name = item[1:-1]
                if node_name in self.pipeline.nodes_dict:
                    selected_nodes.add(node_name)
                    selected_nodes.update(nx.ancestors(dag, node_name))
                    selected_nodes.update(nx.descendants(dag, node_name))
                else:
                    # Check if it's a group or tag
                    group_or_tag_nodes = [
                        n.name
                        for n in self.pipeline.nodes
                        if n.group == node_name or node_name in n.tags
                    ]
                    if not group_or_tag_nodes:
                        raise ValueError(f"Invalid node, group, or tag: {node_name}")
                    for group_node in group_or_tag_nodes:
                        selected_nodes.add(group_node)
                        selected_nodes.update(nx.ancestors(dag, group_node))
                        selected_nodes.update(nx.descendants(dag, group_node))
            elif item.startswith("*"):
                # Node + upstream dependencies
                node_name = item[1:]
                if node_name in self.pipeline.nodes_dict:
                    selected_nodes.add(node_name)
                    selected_nodes.update(nx.ancestors(dag, node_name))
                else:
                    # Check if it's a group or tag
                    group_or_tag_nodes = [
                        n.name
                        for n in self.pipeline.nodes
                        if n.group == node_name or node_name in n.tags
                    ]
                    if not group_or_tag_nodes:
                        raise ValueError(f"Invalid node, group, or tag: {node_name}")
                    for group_node in group_or_tag_nodes:
                        selected_nodes.add(group_node)
                        selected_nodes.update(nx.ancestors(dag, group_node))
            elif item.endswith("*"):
                # Node + downstream dependencies
                node_name = item[:-1]
                if node_name in self.pipeline.nodes_dict:
                    selected_nodes.add(node_name)
                    selected_nodes.update(nx.descendants(dag, node_name))
                else:
                    # Check if it's a group or tag
                    group_or_tag_nodes = [
                        n.name
                        for n in self.pipeline.nodes
                        if n.group == node_name or node_name in n.tags
                    ]
                    if not group_or_tag_nodes:
                        raise ValueError(f"Invalid node, group, or tag: {node_name}")
                    for group_node in group_or_tag_nodes:
                        selected_nodes.add(group_node)
                        selected_nodes.update(nx.descendants(dag, group_node))
            else:
                # Node only
                if item in self.pipeline.nodes_dict:
                    selected_nodes.add(item)
                else:
                    # Check if it's a group or tag
                    group_or_tag_nodes = [
                        n.name
                        for n in self.pipeline.nodes
                        if n.group == item or item in n.tags
                    ]
                    if not group_or_tag_nodes:
                        raise ValueError(f"Invalid node, group, or tag: {item}")
                    selected_nodes.update(group_or_tag_nodes)

        # Sort nodes based on DAG topological order
        sorted_node_names = [
            node for node in self.pipeline.sorted_node_names if node in selected_nodes
        ]

        return sorted_node_names
