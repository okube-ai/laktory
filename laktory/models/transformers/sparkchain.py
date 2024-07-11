from typing import Union
from typing import Literal

from laktory._logger import get_logger
from laktory.models.transformers.basechain import BaseChain
from laktory.models.transformers.sparkchainnode import SparkChainNode

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class SparkChain(BaseChain):
    """
    The `SparkChain` class defines a series of Spark transformation to be
    applied to a dataframe. Each transformation is expressed as a node
    (`SparkChainNode` object) that, upon execution, returns a new dataframe.
    Each node is executed sequentially in the provided order. A node may also
    be another `SparkChain`.

    Attributes
    ----------
    dataframe_type:
        Differentiator to select dataframe chain type
    nodes:
        The list of transformations to be executed.

    Examples
    --------
    ```py
    import pandas as pd
    from laktory import models

    df0 = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 3]}))

    # Build Chain
    sc = models.SparkChain(
        nodes=[
            {
                "with_column": {
                    "name": "cos_x",
                    "type": "double",
                    "expr": "F.cos('x')",
                },
            },
            {
                "nodes": [
                    {
                        "func_name": "withColumnRenamed",
                        "func_args": [
                            "x",
                            "x_tmp",
                        ],
                    },
                    {
                        "with_column": {
                            "name": "x2",
                            "type": "double",
                            "expr": "F.sqrt('x_tmp')",
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

    dataframe_type: Literal["SPARK"] = "SPARK"
    nodes: list[Union[SparkChainNode, "SparkChain"]]
    _columns: list[list[str]] = []
    _parent: "PipelineNode" = None

    @property
    def user_dftype(self):
        return "SPARK"


SparkChain.model_rebuild()
