from typing import TYPE_CHECKING

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.dataframe.dataframeexpr import DataFrameExpr
from laktory.models.dataframe.dataframemethod import DataFrameMethod
from laktory.models.pipelinechild import PipelineChild
from laktory.typing import AnyFrame
from laktory.typing import tagged_union

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


def _infer_transformer_node(data: dict) -> type:
    if "expr" in data:
        return DataFrameExpr
    return DataFrameMethod


DataFrameTransformerNode = tagged_union(
    [DataFrameMethod, DataFrameExpr], _infer_transformer_node, "transformer node"
)

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

    nodes: list[DataFrameTransformerNode] = Field(
        ..., description="List of transformations"
    )

    @property
    def _resolved_nodes(self) -> list[BaseModel]:
        # Skip `${...}` placeholders (the list or an item) not yet injected
        return [n for n in self.nodes if isinstance(n, BaseModel)]

    @property
    def upstream_node_names(self) -> list[str]:
        """Pipeline node names required to apply transformer"""
        names = []
        for node in self._resolved_nodes:
            names += node.upstream_node_names
        return names

    @property
    def data_sources(self):
        """Get all sources feeding the Transformer"""

        sources = []
        for node in self._resolved_nodes:
            sources += node.data_sources

        return sources

    @property
    def view_definition(self):
        if not self.is_sql_expressible:
            return None
        return self.nodes[0]

    @property
    def is_sql_expressible(self) -> bool:
        if len(self.nodes) != 1 or len(self._resolved_nodes) != 1:
            return False
        if not self.nodes[0].is_sql_expressible:
            return False
        return True

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
                df = node.execute(df, named_dfs=named_dfs)
            elif isinstance(node, DataFrameExpr):
                # named_dfs first so the flowing df always wins on key collision
                dfs = dict(named_dfs)
                if df is not None:
                    dfs["df"] = df
                df = node.to_df(dfs)
            else:
                raise NotImplementedError()

        return df


# BaseModel.model_rebuild()
