from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.dataframe.dataframemethod import DataFrameMethod
from laktory.models.dataframe.dataframesqlexpr import DataFrameSQLExpr
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.typing import AnyFrame

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class DataFrameTransformer(BaseModel, PipelineChild):
    """
    A chain of transformations to be applied to a DataFrame. Transformations can be
    SQL- or DataFrame API-based.

    Examples
    --------
    ```py
    import polars as pl
    from laktory import models

    df0 = pl.DataFrame(
        {
            "id": ["a", "b", "c"],
            "x1": [1, 2, 3],
        }
    )

    node0 = DataFrameMethod(
        name="with_columns",
        kwargs={
            "y1": "x1",
        },
    )
    node1 = DataFrameSQLExpr(sql_expr="select id, x1, y1 from df")
    transformer = DataFrameTransformer(nodes=[node0, node1])

    df = transformer.execute(df0)

    print(df)
    ```
    """

    nodes: list[DataFrameMethod | DataFrameSQLExpr] = Field(
        ..., description="List of transformations"
    )
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
    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def child_attribute_names(self):
        return ["nodes"]

    # ----------------------------------------------------------------------- #
    # Execution                                                               #
    # ----------------------------------------------------------------------- #

    def execute(self, df, udfs=None, **named_dfs) -> AnyFrame:
        """
        Execute transformation nodes on provided DataFrame `df`

        Parameters
        ----------
        df:
            Input dataframe
        udfs:
            User-defined functions
        named_dfs:
            Other DataFrame(s) to be passed to the method.

        Returns
        -------
            Output dataframe
        """
        logger.info("Executing DataFrame Transformer")

        for inode, node in enumerate(self.nodes):
            tnode = type(node)
            logger.info(
                f"Executing DataFrame transformer node {inode} ({tnode.__name__})."
            )

            if isinstance(node, DataFrameMethod):
                df = node.execute(df, udfs=udfs, **named_dfs)
            else:
                df = node.execute(df, **named_dfs)

        return df

    #
    # def get_view_definition(self):
    #     logger.info("Creating view definition")
    #     return self.nodes[0].get_view_definition()


# BaseModel.model_rebuild()
