import inspect
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
    df = sc.execute(df0, spark)
    ```
    """
    nodes: list[Union[SparkDataFrameNode, "SparkChain", SparkColumnNode]]

    # TODO: Add validation that spark_func_name is provided for spark dataframe
    # nodes

    def execute(self, df, udfs=None, spark=None) -> DataFrame:
        logger.info("Executing Spark chain")

        for inode, node in enumerate(self.nodes):

            tnode = type(node)
            logger.info(f"Executing node {inode} ({tnode.__name__}).")

            if isinstance(node, SparkChain):
                df = node.execute(df, udfs=udfs, spark=spark)

            elif isinstance(node, SparkColumnNode):
                col = node.execute(df, udfs=udfs)
                df = df.withColumn(node.name, col)

            elif isinstance(node, SparkDataFrameNode):
                df = node.execute(df, udfs=udfs, spark=spark)


        #
        #
        # # Build columns
        # self._columns_to_build = [c for c in self.columns]
        # column_names = [c.name for c in self._columns_to_build]
        # df = self.build_columns(
        #     df, udfs=udfs, raise_exception=not (self.has_joins or self.has_aggregation)
        # )
        #
        # # Execute unions
        # logger.info(f"Executing unions...")
        # for i, union in enumerate(self.unions):
        #     if i == 0:
        #         name = self.source.name
        #     else:
        #         name = "previous_union"
        #     union.left = TableDataSource(name=name)
        #     union.left._df = df
        #     df = union.execute(spark)
        #
        # # Execute joins
        # logger.info(f"Executing joins...")
        # for i, join in enumerate(self.joins):
        #     if i == 0:
        #         name = self.source.name
        #     else:
        #         name = "previous_join"
        #     join.left = TableDataSource(name=name)
        #     join.left._df = df
        #     df = join.execute(spark)
        #
        #     # Build remaining columns again (in case inputs are found in joins)
        #     df = self.build_columns(
        #         df, udfs=udfs, raise_exception=i == len(self.joins) - 1
        #     )
        #
        # # Add layer columns
        # logger.info(f"Adding layer columns...")
        # layer_columns = self._get_layer_columns(layer=self.layer, df=df)
        # self._columns_to_build += layer_columns
        # column_names += [c.name for c in layer_columns]
        # df = self.build_columns(
        #     df,
        #     udfs=udfs,
        #     raise_exception=not (
        #         self.has_joins_post_aggregation or self.has_aggregation
        #     ),
        # )
        #
        # # Window filtering
        # if self.window_filter:
        #     logger.info(f"Window filtering...")
        #     df = self.window_filter.execute(df)
        #
        # # Drop source columns
        # if self.drop_source_columns:
        #     logger.info(f"Dropping source columns...")
        #     df = df.select(column_names)
        #
        # if self.aggregation:
        #     logger.info(f"Executing Aggregations...")
        #     df = self.aggregation.execute(df, udfs=udfs)
        #     self._columns_to_build += self._get_layer_columns(layer=self.layer, df=df)
        #
        # # Build columns after aggregation
        # df = self.build_columns(
        #     df, udfs=udfs, raise_exception=not self.has_joins_post_aggregation
        # )
        #
        # # Make post-aggregation joins
        # for i, join in enumerate(self.joins_post_aggregation):
        #     logger.info(f"Post-aggregation joins...")
        #     if i == 0:
        #         name = self.source.name
        #     else:
        #         name = "previous_join"
        #     join.left = TableDataSource(name=name)
        #     join.left._df = df
        #     df = join.execute(spark)
        #
        #     # Build remaining columns again (in case inputs are found in joins)
        #     df = self.build_columns(
        #         df, udfs=udfs, raise_exception=i == len(self.joins) - 1
        #     )
        #
        # # Apply filter
        # if self.filter:
        #     df = df.filter(self.filter)
        #
        # # Select columns
        # cols = []
        # if self.selects:
        #     logger.info(f"Selecting columns...")
        #     if isinstance(self.selects, list):
        #         cols += [F.col(c) for c in self.selects]
        #     elif isinstance(self.selects, dict):
        #         cols += [F.col(k).alias(v) for k, v in self.selects.items()]
        #     df = df.select(cols)
        #
        # # Drop columns
        # if self.drop_columns:
        #     logger.info(f"Dropping columns {self.drop_columns}...")
        #     df = df.drop(*self.drop_columns)
        #
        # # Drop duplicates
        # if self.drop_duplicates:
        #     subset = None
        #     if isinstance(self.drop_duplicates, list):
        #         subset = self.drop_duplicates
        #     elif self.primary_key:
        #         subset = [self.primary_key]
        #
        #     logger.info(f"Removing duplicates with {subset}")
        #     df = df.dropDuplicates(subset)

        return df


SparkChain.model_rebuild()


def builtin_dataframe_functions():

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

