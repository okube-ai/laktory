from typing import TYPE_CHECKING

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.dataframe.dataframeexpr import DataFrameExpr
from laktory.models.dataframe.dataframemethod import DataFrameMethod
from laktory.models.pipelinechild import PipelineChild
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

    import laktory as lk

    df0 = pl.DataFrame(
        {
            "id": ["a", "b", "c"],
            "x1": [1, 2, 3],
        }
    )

    node0 = lk.models.DataFrameMethod(
        func_name="with_columns",
        func_kwargs={
            "y1": "x1",
        },
    )
    node1 = lk.models.DataFrameExpr(expr="select id, x1, y1 from {df}")
    transformer = lk.models.DataFrameTransformer(nodes=[node0, node1])

    df = transformer.execute(df0).collect()

    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    | | id | x1 | y1 | |
    | |----|----|----| |
    | | a  | 1  | 1  | |
    | | b  | 2  | 2  | |
    | | c  | 3  | 3  | |
    └──────────────────┘
    '''
    ```
    References
    ----------
    * [Data Transformer](https://www.laktory.ai/concepts/transformer/)
    """

    nodes: list[DataFrameMethod | DataFrameExpr] = Field(
        ..., description="List of transformations"
    )

    @property
    def upstream_node_names(self) -> list[str]:
        """Pipeline node names required to apply transformer"""
        names = []
        for node in self.nodes:
            names += node.upstream_node_names
        return names

    @property
    def data_sources(self):
        """Get all sources feeding the Transformer"""

        sources = []
        for node in self.nodes:
            sources += node.data_sources

        return sources

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return ["nodes"]

    # ----------------------------------------------------------------------- #
    # Execution                                                               #
    # ----------------------------------------------------------------------- #

    def execute(self, df, named_dfs=None) -> AnyFrame:
        """
        Execute transformation nodes on provided DataFrame `df`

        Parameters
        ----------
        df:
            Input dataframe
        named_dfs:
            Other DataFrame(s) to be passed to the method.

        Returns
        -------
            Output dataframe
        """
        logger.info("Executing DataFrame Transformer")

        if named_dfs is None:
            named_dfs = {}

        for inode, node in enumerate(self.nodes):
            tnode = type(node)
            logger.info(
                f"Executing DataFrame transformer node {inode} ({tnode.__name__})."
            )

            if isinstance(node, DataFrameMethod):
                df = node.execute(df)
            elif isinstance(node, DataFrameExpr):
                dfs = {}
                if df is not None:
                    dfs["df"] = df
                dfs = dfs | named_dfs
                df = node.to_df(dfs)
            else:
                raise NotImplementedError()

        return df


# BaseModel.model_rebuild()
