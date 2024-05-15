from typing import Union
from typing import Literal

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
# from laktory.models.spark.sparkchainnode import SparkChainNode
# from laktory.spark import SparkDataFrame
from laktory.models.pipelinenode import PipelineNode
from laktory.models.databricks.pipeline import Pipeline

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

    def plan(self) -> None:
        pass

    def execute(self, spark, udfs=None) -> None:
        logger.info("Executing Pipeline")

        for inode, node in enumerate(self.nodes):
            logger.info(f"Executing node {inode} ({node.id}).")
            node.execute(spark, udfs=udfs)
