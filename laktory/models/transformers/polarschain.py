from typing import Any
from typing import Union
from pydantic import model_validator

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
    The `PolarsChain` class defines a series of Polars transformation to be
    applied to a dataframe. Each transformation is expressed as a node
    (`PolarsChainNode` object) that, upon execution, returns a new dataframe.
    Each node is executed sequentially in the provided order. A node may also
    be another `PolarsChain`.

    Attributes
    ----------
    nodes:
        The list of transformations to be executed.
    polars:
        Dummy configuration attribute to identify chain as Polars

    Examples
    --------
    ```py
    import polars as pl
    from laktory import models

    df0 = pl.DataFrame({"x": [1, 2, 3]})

    # Build Chain
    sc = models.PolarsChain(
        polars=True,
        nodes=[
            {
                "with_column": {
                    "name": "cos_x",
                    "type": "double",
                    "expr": "pl.col('x').cos()",
                },
            },
            {
                "nodes": [
                    {
                        "func_name": "rename",
                        "func_args": [
                            {"x": "x_tmp"},
                        ],
                    },
                    {
                        "with_column": {
                            "name": "x2",
                            "type": "double",
                            "expr": "pl.col('x_tmp').sqrt()",
                        },
                    },
                ],
            },
            {
                "func_name": "drop",
                "func_args": [
                    "x_tmp",
                ],
            },
        ],
    )

    # Execute Chain
    df = sc.execute(df0)

    # Print result
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 3
    Columns: 2
    $ cos_x <f64> 0.5403023058681398, -0.4161468365471424, -0.9899924966004454
    $ x2    <f64> 1.0, 1.4142135623730951, 1.7320508075688772
    '''
    ```
    """

    nodes: list[Union[PolarsChainNode, "PolarsChain"]]
    polars: bool = True
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

    @property
    def user_dftype(self) -> Union[str, None]:
        """
        User-configured dataframe type directly from model or from parent.
        """
        return "POLARS"
        # if "dataframe_type" in self.__fields_set__:
        #     return self.dataframe_type
        # if self._parent:
        #     return self._parent.user_dftype
        # return None

    def execute(self, df, udfs=None) -> PolarsDataFrame:
        logger.info("Executing Polars chain")

        for inode, node in enumerate(self.nodes):
            self._columns += [df.columns]

            tnode = type(node)
            logger.info(f"Executing polars chain node {inode} ({tnode.__name__}).")
            df = node.execute(df, udfs=udfs)

        return df


PolarsChain.model_rebuild()
