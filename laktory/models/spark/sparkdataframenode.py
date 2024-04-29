from pydantic import field_validator
from typing import Union
from typing import Any
from typing import Callable

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.spark.sparkfuncarg import SparkFuncArg
from laktory.spark import DataFrame
from laktory.spark import Column as SparkColumn
from laktory.models.spark._common import parse_args
from laktory.models.spark._common import parse_kwargs
from laktory.exceptions import MissingColumnError
from laktory.exceptions import MissingColumnsError

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class SparkDataFrameNode(BaseModel):
    """
    SparkChain node that output a DataFrame upon execution.

    Attributes
    ----------
    name:
        Name of the column
    allow_missing_column_args:
        If `True`, spark func column arguments are allowed to be missing
        without raising an exception.
    spark_func_args:
        List of arguments to be passed to the spark function to build the
        column.
        To support spark functions expecting column argument, col("x"),
        lit("3") and expr("x*2") can be provided.
        To support spark functions expecting DataFrame, a dict representation
        of a laktory.models.TableDataSource can be provided.
    spark_func_kwargs:
        List of keyword arguments to be passed to the spark function to build
        the column.
        To support spark functions expecting column argument, col("x"),
        lit("3") and expr("x*2") can be provided.
        To support spark functions expecting DataFrame, a dict representation
        of a laktory.models.TableDataSource can be provided.
    spark_func_name:
        Name of the spark function to build the column. Mutually exclusive to
        `sql_expression`
    sql_expression:
        Expression defining how to build the column. Mutually exclusive to
        `spark_func_name`
    type:
        Column data type
    unit:
        Column units

    Examples
    --------
    ```py
    from laktory import models

    df0 = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 2, 3]}))

    node = models.SparkDataFrameNode(
        spark_func_name="drop_duplicates",
        spark_func_args=[["x"]],
    )
    df = node.execute(df, spark)
    ```
    """

    allow_missing_column_args: Union[bool, None] = False
    spark_func_args: list[Union[Any, SparkFuncArg]] = []
    spark_func_kwargs: dict[str, Union[Any, SparkFuncArg]] = {}
    spark_func_name: Union[str, None]
    # sql_expression: Union[str, None] = None

    parse_args = parse_args
    parse_kwargs = parse_kwargs

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    def execute(
        self,
        df: DataFrame,
        udfs: list[Callable[[...], SparkColumn]] = None,
        spark=None,
    ) -> DataFrame:
        """
        Build Spark Column

        Parameters
        ----------
        df:
            Input DataFrame
        udfs:
            User-defined functions
        spark:
            Spark Session

        Returns
        -------
            Output DataFrame
        """
        from pyspark.sql import DataFrame
        from pyspark.sql import Column
        from laktory.spark.dataframe import has_column
        from laktory.spark import DataFrame as LDF

        if udfs is None:
            udfs = []
        udfs = {f.__name__: f for f in udfs}

        # TODO: From SQL expression
        # if self.sql_expression:
        #     raise NotImplementedError("sql_expression not supported yet")
        #
        #     df.createOrReplaceTempView("_df")
        #     df = spark.sql(self.sql_expression)
        #     return df

        # From Spark Function
        func_name = self.spark_func_name
        if self.spark_func_name is None:
            raise ValueError(
                "`spark_func_name` must be specific for a spark dataframe node"
            )

        # Get from UDFs
        f = udfs.get(func_name, None)
        if f is None:
            # Get from built-in spark functions
            f = getattr(DataFrame, func_name, None)

            if f is None:
                # Get from laktory functions
                f = getattr(LDF, func_name, None)

        if f is None:
            raise ValueError(f"Function {func_name} is not available")

        _args = self.spark_func_args
        _kwargs = self.spark_func_kwargs

        # Build log
        func_log = f"{func_name}("
        for a in _args:
            func_log += f"{a.value},"
        for k, a in _kwargs.items():
            func_log += f"{k}={a.value},"
        if func_log.endswith(","):
            func_log = func_log[:-1]
        func_log += ")"
        logger.info(f"df as {func_log}")

        # Build args
        args = []
        missing_column_names = []
        for i, _arg in enumerate(_args):
            parg = _arg.eval(spark)
            cname = _arg.value

            if df is not None:
                # Check if explicitly defined columns are available
                if isinstance(parg, Column):
                    cname = str(parg).split("'")[1]
                    if not has_column(df, cname):
                        missing_column_names += [cname]
                        if not self.allow_missing_column_args:
                            logger.error(
                                f"Input column {cname} is missing. Abort building {self.name}"
                            )
                            raise MissingColumnError(cname)
                        else:
                            logger.warn(
                                f"Input column {cname} is missing for building {self.name}"
                            )
                            continue

                else:
                    pass
                    # TODO: We could try to check if the function expect columns and if it's the case assume
                    # that a string argument is intended to be a column name, but I feel this will be a rabbit
                    # hole with tons of exceptions.

            args += [parg]

        if len(_args) > 0 and len(args) < 1:
            raise MissingColumnsError(missing_column_names)

        # Build kwargs
        kwargs = {}
        for k, _arg in _kwargs.items():
            kwargs[k] = _arg.eval(spark)

        # Function call
        logger.info(f"df as {func_name}({args}, {kwargs})")
        df = f(df, *args, **kwargs)

        return df
