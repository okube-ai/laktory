from typing import Union
from typing import Literal

from laktory._logger import get_logger
from laktory.models.transformers.basechain import BaseChain
from laktory.models.transformers.polarschainnode import PolarsChainNode

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class PolarsChain(BaseChain):
    """
    The `PolarsChain` class defines a series of Polars transformation to be
    applied to a dataframe. Each transformation is expressed as a node
    (`PolarsChainNode` object) that, upon execution, returns a new dataframe.
    Each node is executed sequentially in the provided order. A node may also
    be another `PolarsChain`.

    Attributes
    ----------
    dataframe_type:
        Differentiator to select dataframe chain type
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

    dataframe_type: Literal["POLARS"] = "POLARS"
    nodes: list[Union[PolarsChainNode, "PolarsChain"]]
    _columns: list[list[str]] = []
    _parent: "PipelineNode" = None

    @property
    def user_dftype(self):
        return "POLARS"


PolarsChain.model_rebuild()
