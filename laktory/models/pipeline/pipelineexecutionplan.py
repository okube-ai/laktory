from collections import defaultdict
from typing import Any

import networkx as nx
from pydantic import Field
from pydantic import SkipValidation
from pydantic import model_validator

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
    A pipeline execution plan defines the pipeline tasks to be executed according to the selected nodes/tasks/tags
    and their dependencies. It constructs a DAG of pipeline tasks, where each task can consist of one or more nodes
    that share the same `execution_task_name`.
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

    @model_validator(mode="after")
    def update_pipeline(self) -> Any:
        if self.pipeline is not None:
            self.pipeline._plan = self
        return self

    # -------------------------------------------------------------------------------- #
    # Nodes                                                                            #
    # -------------------------------------------------------------------------------- #

    @property
    def node_names(self) -> list[str]:
        """Selected pipeline node names"""
        # TODO: Refactor as a cached property??
        if self.selects is None:
            return self.pipeline.sorted_node_names

        # Validate and resolve selected nodes/tasks/tags
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
                    # Check if it's a task or tag
                    task_or_tag_nodes = [
                        n.name
                        for n in self.pipeline.nodes
                        if n.execution_task_name == node_name or node_name in n.tags
                    ]
                    if not task_or_tag_nodes:
                        raise ValueError(
                            f"Invalid node, execution_task_name, or tag: {node_name}"
                        )
                    for group_node in task_or_tag_nodes:
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
                    # Check if it's a task or tag
                    task_or_tag_nodes = [
                        n.name
                        for n in self.pipeline.nodes
                        if n.execution_task_name == node_name or node_name in n.tags
                    ]
                    if not task_or_tag_nodes:
                        raise ValueError(
                            f"Invalid node, execution_task_name, or tag: {node_name}"
                        )
                    for group_node in task_or_tag_nodes:
                        selected_nodes.add(group_node)
                        selected_nodes.update(nx.ancestors(dag, group_node))
            elif item.endswith("*"):
                # Node + downstream dependencies
                node_name = item[:-1]
                if node_name in self.pipeline.nodes_dict:
                    selected_nodes.add(node_name)
                    selected_nodes.update(nx.descendants(dag, node_name))
                else:
                    # Check if it's a task or tag
                    task_or_tag_nodes = [
                        n.name
                        for n in self.pipeline.nodes
                        if n.execution_task_name == node_name or node_name in n.tags
                    ]
                    if not task_or_tag_nodes:
                        raise ValueError(
                            f"Invalid node, execution_task_name, or tag: {node_name}"
                        )
                    for group_node in task_or_tag_nodes:
                        selected_nodes.add(group_node)
                        selected_nodes.update(nx.descendants(dag, group_node))
            else:
                # Node only
                if item in self.pipeline.nodes_dict:
                    selected_nodes.add(item)
                else:
                    # Check if it's a task or tag
                    task_or_tag_nodes = [
                        n.name
                        for n in self.pipeline.nodes
                        if n.execution_task_name == item or item in n.tags
                    ]
                    if not task_or_tag_nodes:
                        raise ValueError(
                            f"Invalid node, execution_task_name, or tag: {item}"
                        )
                    selected_nodes.update(task_or_tag_nodes)

        # Sort nodes based on DAG topological order
        sorted_node_names = [
            node for node in self.pipeline.sorted_node_names if node in selected_nodes
        ]

        return sorted_node_names

    @property
    def nodes_dict(self):
        """Selected pipeline nodes dict"""
        return {k: self.pipeline.nodes_dict[k] for k in self.node_names}

    @property
    def nodes_dag(self) -> nx.DiGraph:
        """Selected pipeline nodes DAG"""
        selected_nodes = set(self.node_names)
        dag = self.pipeline.dag
        for node_name in list(dag.nodes.keys()):
            if node_name not in selected_nodes:
                dag.remove_node(node_name)

        return dag

    # -------------------------------------------------------------------------------- #
    # Tasks                                                                            #
    # -------------------------------------------------------------------------------- #

    @property
    def dag(self) -> nx.DiGraph:
        """
        Constructs a DAG for the execution plan, where each node is either:
        - A single pipeline node (default `execute_task_name`), or
        - A group of nodes (if they share the same `execute_task_name`).

        Pipeline nodes without sink(s) are omitted unless they are leaf (terminal) nodes
        as they will be executed implicitly when executing their downstream nodes.

        Returns
        -------
        nx.DiGraph
            A directed acyclic graph representing the execution plan.
        """
        nodes_dag = self.nodes_dag
        nodes_dict = self.nodes_dict

        def is_valid(node):
            return node.has_sinks or nodes_dag.out_degree[node.name] == 0

        # Create a mapping of execution nodes (groups or standalone) to their pipeline nodes
        task_nodes = defaultdict(list)

        for node in nodes_dict.values():
            if not is_valid(node):
                continue

            task_name = f"{node.execution_task_name}"
            task_nodes[task_name].append(node.name)

        # Initialize a new DAG
        dag = nx.DiGraph()

        # Add execution nodes
        for task_name, node_names in task_nodes.items():
            dag.add_node(task_name, node_names=node_names)

        # Map each included pipeline node to its task name
        nodes_task_name = {}
        for task_name, node_names in task_nodes.items():
            for node_name in node_names:
                nodes_task_name[node_name] = task_name

        def resolve_upstream_task_names(node_name: str) -> list[str]:
            """
            Walk upstream from start_name, skipping selected nodes without sinks,
            until we reach selected nodes with sinks (i.e., execution nodes).
            """
            resolved = set()
            stack = list(nodes_dag.predecessors(node_name))
            seen = set()

            while stack:
                pred_name = stack.pop()
                if pred_name in seen:
                    continue
                seen.add(pred_name)

                pred_node = nodes_dict.get(pred_name)
                if pred_node is None:
                    # Shouldn't happen if nodes_dag is consistent, but be safe.
                    continue

                if is_valid(pred_node):
                    # This upstream node is a task node (group or standalone)
                    task_name = nodes_task_name.get(pred_name)
                    if task_name is not None:
                        resolved.add(task_name)
                    continue

                # Not a task node: skip through it (it will run implicitly downstream)
                stack.extend(list(nodes_dag.predecessors(pred_name)))

            return list(resolved)

        # Add edges
        for task_name, node_names in task_nodes.items():
            upstream_task_names = []
            for node_name in node_names:
                upstream_task_names += resolve_upstream_task_names(node_name)
            upstream_task_names = list(set(upstream_task_names))

            for upstream_task_name in upstream_task_names:
                if upstream_task_name != task_name:
                    dag.add_edge(upstream_task_name, task_name)

        return dag

    @property
    def tasks(self):
        """
        List of pipeline tasks to be executed. Each task is either:
        - A single pipeline node (default `execute_task_name`), or
        - A group of nodes (if they share the same `execute_task_name`).

        Pipeline nodes without sink(s) are omitted unless they are leaf (terminal) nodes
        as they will be executed implicitly when executing their downstream nodes.

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
