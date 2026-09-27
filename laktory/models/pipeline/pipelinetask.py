from pydantic import Field
from pydantic import SkipValidation

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.pipeline._execute import _execute  # noqa: F401
from laktory.models.pipeline._update_tables_metadata import (
    _update_tables_metadata,  # noqa: F401
)
from laktory.models.pipeline.pipeline import Pipeline
from laktory.typing import AnyFrame

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Functions                                                            #
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class PipelineTask(BaseModel):
    """
    A pipeline task is a unit of execution within a pipeline, defined by a set of nodes to be executed together.
    """

    name: str = Field(
        ...,
        description="""Pipeline task name""",
    )
    pipeline: SkipValidation[Pipeline] = Field(
        ...,
        description="""Pipeline""",
    )
    node_names: list[str] = Field(
        ..., description="""List of node names in sorted order of execution"""
    )

    def execute(
        self,
        write_sinks=True,
        full_refresh: bool = False,
        named_dfs: dict[str, AnyFrame] = None,
        update_tables_metadata: bool = True,
        reset_mode: str | None = None,
        reset_only: bool = False,
    ) -> None:
        """
        Execute the pipeline task.

        Parameters
        ----------
        write_sinks:
            If `False` writing of node sinks will be skipped
        full_refresh:
            If `True` all nodes will be completely re-processed by deleting
            existing data and checkpoints before processing.
        named_dfs:
            Named DataFrames to be passed to pipeline nodes transformer.
        update_tables_metadata:
            Update tables metadata
        reset_mode:
            Optional override for sinks `reset_mode` when `full_refresh` or
            `reset_only` is `True`.
        reset_only:
            If `True`, the sinks of the task nodes are only reset, without reading or
            writing data.
        """

        logger.info(f"Executing pipeline task '{self.name}'")

        # Targets written by multiple nodes of this task are purged once, by the first
        # writer, before any of them writes.
        purged_targets = set()
        grouped = sorted({t for n in self.nodes for t in n.grouped_sink_targets})
        if grouped:
            logger.info(
                f"Nodes {self.node_names} write to the same targets {grouped} and are "
                "executed together."
            )

        # Execute nodes
        for node_name in self.node_names:
            node = self.pipeline.nodes_dict[node_name]
            if named_dfs is None:
                named_dfs = {}

            if reset_only:
                node.purge(mode=reset_mode, purged_targets=set(purged_targets))
                purged_targets |= node.grouped_sink_targets
                continue

            node.execute(
                write_sinks=write_sinks,
                full_refresh=full_refresh,
                named_dfs=named_dfs,
                update_tables_metadata=update_tables_metadata,
                reset_mode=reset_mode,
                purged_targets=set(purged_targets),
            )
            purged_targets |= node.grouped_sink_targets

    @property
    def upstream_task_names(self) -> list[str]:
        """Get upstream task names"""
        plan = self.pipeline._plan
        names = []
        for edges in plan.dag.in_edges(self.name):
            if edges[0] != self.name:
                names += [edges[0]]
        return names

    @property
    def nodes(self):
        """Task nodes"""
        return [self.pipeline.nodes_dict[node_name] for node_name in self.node_names]

    @property
    def has_sinks(self) -> bool:
        """`True` if at least one sink is found in task nodes."""
        has_sinks = False
        for node in self.nodes:
            if node.has_sinks:
                has_sinks = True
                break
        return has_sinks

    @property
    def is_sql_expressible(self) -> bool:
        """`True` if all nodes in the task can run on a SQL warehouse."""
        return all(node.is_sql_expressible for node in self.nodes)
