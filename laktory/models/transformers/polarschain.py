from typing import Union

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.transformers.polarschainnode import PolarsChainNode
from laktory.polars import PolarsDataFrame

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class PolarsChain(BaseModel):
    """
    The Polars Chain class defines a series of transformation to be applied to
    a dataframe. Each transformation is expressed as a node (PolarsChainNode
    object) that, upon execution, returns a new dataframe. As a convenience,
    `column` can be specified to create a new column. In this case, the polars
    function or sql expression is expected to return a column instead of a
    dataframe. Each node is executed sequentially in the provided order. A node
    may also be another Polars Chain.

    Attributes
    ----------
    nodes:
        The list of transformations to be executed.

    Examples
    --------
    ```py
    import polars as pl
    from laktory import models

    df0 = pl.DataFrame({"x": [1, 2, 3]})

    # Build Chain
    sc = models.PolarsChain(
        nodes=[
            {
                "column": {
                    "name": "cos_x",
                    "type": "double",
                },
                "polars_func_name": "cos",
                "polars_func_args": ["col('x')"],
            },
            {
                "nodes": [
                    {
                        "polars_func_name": "rename",
                        "polars_func_args": [
                            {"x": "x_tmp"},
                        ],
                    },
                    {
                        "column": {
                            "name": "x2",
                            "type": "double",
                        },
                        "polars_func_name": "sqrt",
                        "polars_func_args": ["col('x_tmp')"],
                    },
                ],
            },
            {
                "polars_func_name": "drop",
                "polars_func_args": [
                    "x_tmp",
                ],
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df0)

    # Print result
    print(df.toPandas().to_string())
    '''
          cos_x        x2
    0  0.540302  1.000000
    1 -0.416147  1.414214
    2 -0.989992  1.732051
    '''
    ```
    """

    nodes: list[Union[PolarsChainNode, "PolarsChain"]]
    _columns: list[list[str]] = []

    @property
    def columns(self):
        return self._columns

    def execute(self, df, udfs=None) -> PolarsDataFrame:
        logger.info("Executing Polars chain")

        for inode, node in enumerate(self.nodes):
            self._columns += [df.columns]

            tnode = type(node)
            logger.info(f"Executing polars chain node {inode} ({tnode.__name__}).")
            df = node.execute(df, udfs=udfs)

        return df


PolarsChain.model_rebuild()
