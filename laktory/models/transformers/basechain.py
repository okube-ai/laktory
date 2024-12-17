from __future__ import annotations
from typing import Any
from typing import Union
from typing import Literal
from typing import TYPE_CHECKING
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.transformers.basechainnode import BaseChainNode
from laktory.types import AnyDataFrame

if TYPE_CHECKING:
    from laktory.models.datasources.basedatasource import BaseDataSource

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class BaseChain(BaseModel):
    dataframe_backend: Literal["SPARK", "POLARS"] = None
    nodes: list[Union[BaseChainNode, "BaseChain"]]
    _columns: list[list[str]] = []
    _parent: "PipelineNode" = None

    @model_validator(mode="after")
    def update_children(self) -> Any:
        for n in self.nodes:
            n._parent = self
        return self

    @property
    def columns(self):
        return self._columns

    def get_sources(self, cls=None) -> list[BaseDataSource]:
        """Get all sources feeding the Polars Chain"""

        from laktory.models.datasources.basedatasource import BaseDataSource

    @property
    def data_sources(self):
        """Get all sources feeding the Transformer"""
        sources = []
        for node in self.nodes:
            sources += node.data_sources
        for s in sources:
            s._parent = self
        return sources

    def execute(self, df, udfs=None) -> AnyDataFrame:
        logger.info(f"Executing {self.df_backend} chain")

        for inode, node in enumerate(self.nodes):
            self._columns += [df.columns]

            tnode = type(node)
            logger.info(
                f"Executing {self.df_backend} chain node {inode} ({tnode.__name__})."
            )
            df = node.execute(df, udfs=udfs)

        return df


BaseModel.model_rebuild()
