from typing import Any
from typing import Union
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.transformers.sparkchainnode import SparkChainNode
from laktory.spark import SparkDataFrame

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class SparkChain(BaseModel):
    """
    The `SparkChain` class defines a series of Spark transformation to be
    applied to a dataframe. Each transformation is expressed as a node
    (`SparkChainNode` object) that, upon execution, returns a new dataframe.
    Each node is executed sequentially in the provided order. A node may also
    be another `SparkChain`.

    Attributes
    ----------
    nodes:
        The list of transformations to be executed.
    spark:
        Dummy configuration attribute to identify chain as Spark

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

    nodes: list[Union[SparkChainNode, "SparkChain"]]
    spark: bool = True
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
        return "SPARK"
        # if "dataframe_type" in self.__fields_set__:
        #     return self.dataframe_type
        # if self._parent:
        #     return self._parent.user_dftype
        # return None

    def execute(self, df, udfs=None) -> SparkDataFrame:
        logger.info("Executing Spark chain")

        for inode, node in enumerate(self.nodes):
            self._columns += [df.columns]

            tnode = type(node)
            logger.info(f"Executing spark chain node {inode} ({tnode.__name__}).")
            df = node.execute(df, udfs=udfs)

        return df


SparkChain.model_rebuild()


def _builtin_dataframe_functions():
    from pyspark.sql import DataFrame
    import inspect

    func_names = []

    for k in vars(DataFrame).keys():
        if k.startswith("_"):
            continue
        m = getattr(DataFrame, k)

        if not (inspect.isfunction(m) or inspect.ismethod(m)):
            continue

        sig = inspect.signature(m)
        return_type = sig.return_annotation

        if return_type == "DataFrame":
            func_names += [k]

    return func_names
