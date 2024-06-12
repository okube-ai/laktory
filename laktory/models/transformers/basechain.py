from __future__ import annotations
from typing import Any
from typing import Union
from typing import Literal
from typing import TYPE_CHECKING
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.constants import DEFAULT_DFTYPE
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
    dataframe_type: Literal["SPARK", "POLARS"] = DEFAULT_DFTYPE
    nodes: list[Union[BaseChainNode, "BaseChain"]]
    _columns: list[list[str]] = []
    _parent: "PipelineNode" = None

    @model_validator(mode="after")
    def update_children(self) -> Any:
        for n in self.nodes:
            n._parent = self
        return self

    @model_validator(mode="after")
    def update_sources(self) -> Any:
        for s in self.get_sources():
            if "dataframe_type" not in s.__fields_set__:
                s.dataframe_type = self.dataframe_type
        return self

    @property
    def columns(self):
        return self._columns

    def get_sources(self, cls=None) -> list[BaseDataSource]:
        """Get all sources feeding the Polars Chain"""

        from laktory.models.datasources.basedatasource import BaseDataSource

        if cls is None:
            cls = BaseDataSource

        sources = []
        for node in self.nodes:
            sources += node.get_sources(cls)
        return sources

    def execute(self, df, udfs=None) -> AnyDataFrame:
        logger.info(f"Executing {self.dataframe_type} chain")

        for inode, node in enumerate(self.nodes):
            self._columns += [df.columns]

            tnode = type(node)
            logger.info(
                f"Executing {self.dataframe_type} chain node {inode} ({tnode.__name__})."
            )
            df = node.execute(df, udfs=udfs)

        return df


BaseModel.model_rebuild()
