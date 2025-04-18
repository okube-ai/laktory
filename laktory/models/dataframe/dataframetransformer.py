from __future__ import annotations

from typing import TYPE_CHECKING

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.dataframe.dataframetransformernode import DataFrameTransformerNode
from laktory.models.pipeline.pipelinechild import PipelineChild

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class DataFrameTransformer(BaseModel, PipelineChild):
    nodes: list[DataFrameTransformerNode]
    # _columns: list[list[str]] = []
    #
    # @property
    # def columns(self):
    #     return self._columns
    #
    # @property
    # def upstream_node_names(self) -> list[str]:
    #     """Pipeline node names required to apply transformer"""
    #     names = []
    #     for node in self.nodes:
    #         names += node.upstream_node_names
    #     return names
    #
    # @property
    # def data_sources(self):
    #     """Get all sources feeding the Transformer"""
    #     sources = []
    #     for node in self.nodes:
    #         sources += node.data_sources
    #     return sources
    #
    # # ----------------------------------------------------------------------- #
    # # Children                                                                #
    # # ----------------------------------------------------------------------- #
    #
    # @property
    # def child_attribute_names(self):
    #     return ["nodes"]
    #
    # # ----------------------------------------------------------------------- #
    # # Execution                                                               #
    # # ----------------------------------------------------------------------- #
    #
    # def execute(self, df, udfs=None) -> AnyDataFrame:
    #     logger.info(f"Executing {self.df_backend} chain")
    #
    #     for inode, node in enumerate(self.nodes):
    #         self._columns += [df.columns]
    #
    #         tnode = type(node)
    #         logger.info(
    #             f"Executing {self.df_backend} chain node {inode} ({tnode.__name__})."
    #         )
    #         df = node.execute(df, udfs=udfs)
    #
    #     return df
    #
    # def get_view_definition(self):
    #     logger.info("Creating view definition")
    #     return self.nodes[0].get_view_definition()


# BaseModel.model_rebuild()
