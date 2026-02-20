from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.pipeline._execute import _execute  # noqa: F401
from laktory.models.pipeline._post_execute import _post_execute  # noqa: F401
from laktory.models.pipeline.pipeline import Pipeline
from laktory.models.pipelinechild import PipelineChild
from laktory.typing import AnyFrame

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Functions                                                            #
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class PipelineTask(BaseModel, PipelineChild):
    """
    A pipeline task is a unit of execution within a pipeline, defined by a set of nodes to be executed together.
    """

    name: str = Field(
        ...,
        description="""Pipeline task name""",
    )
    pipeline: Pipeline = Field(
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
        """

        logger.info(f"Executing pipeline task '{self.name}'")

        # Execute nodes
        for node_name in self.node_names:
            node = self.pipeline.nodes_dict[node_name]
            if named_dfs is None:
                named_dfs = {}

            node.execute(
                write_sinks=write_sinks,
                full_refresh=full_refresh,
                named_dfs=named_dfs,
                update_tables_metadata=update_tables_metadata,
            )
