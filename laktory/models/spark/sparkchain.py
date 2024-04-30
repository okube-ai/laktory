from typing import Union

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.spark.sparkdataframenode import SparkDataFrameNode
from laktory.models.spark.sparkcolumnnode import SparkColumnNode
from laktory.spark import DataFrame

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class SparkChain(BaseModel):
    """
    The Spark Chain class defines a series of transformation to be applied to
    a DataFrame. Each transformation is expressed by a node that can either
    add a new column (SparkColumnNode) or by a node that returns a new
    DataFrame entirely (SparkDataFrameNode). Each node is executed
    sequentially in the provided order. Each node may also be another Spark
    Chain.

    Attributes
    ----------
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
                "name": "cos_x",
                "type": "double",
                "spark_func_name": "cos",
                "spark_func_args": ["x"],
            },
            {
                "nodes": [
                    {
                        "spark_func_name": "withColumnRenamed",
                        "spark_func_args": [
                            "x",
                            "x_tmp",
                        ],
                    },
                    {
                        "name": "x2",
                        "type": "double",
                        "spark_func_name": "sqrt",
                        "spark_func_args": ["x_tmp"],
                    },
                ],
            },
            {
                "spark_func_name": "drop",
                "spark_func_args": [
                    "x_tmp",
                ],
            },
        ]
    )

    # Execute Chain
    df = sc.execute(df0, spark=spark)

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

    nodes: list[Union[SparkDataFrameNode, "SparkChain", SparkColumnNode]]
    _columns: list[list[str]] = []

    @property
    def columns(self):
        return self._columns

    def execute(self, df, udfs=None, spark=None) -> DataFrame:
        logger.info("Executing Spark chain")

        for inode, node in enumerate(self.nodes):
            self._columns += [df.columns]

            tnode = type(node)
            logger.info(f"Executing node {inode} ({tnode.__name__}).")

            if isinstance(node, SparkChain):
                df = node.execute(df, udfs=udfs, spark=spark)

            elif isinstance(node, SparkColumnNode):
                col = node.execute(df, udfs=udfs)
                df = df.withColumn(node.name, col)

            elif isinstance(node, SparkDataFrameNode):
                df = node.execute(df, udfs=udfs, spark=spark)

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
