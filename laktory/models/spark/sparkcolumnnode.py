from pydantic import field_validator
from typing import Any
from typing import Union
from typing import Callable

from laktory._logger import get_logger
from laktory.constants import SUPPORTED_TYPES
from laktory.models.basemodel import BaseModel
from laktory.models.spark.sparkfuncarg import SparkFuncArg
from laktory.spark import DataFrame
from laktory.spark import Column
from laktory.models.spark._common import parse_args
from laktory.models.spark._common import parse_kwargs
from laktory.exceptions import MissingColumnError
from laktory.exceptions import MissingColumnsError

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class SparkColumnNode(BaseModel):
    """
    SparkChain node that output a column upon execution.

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
    spark_func_kwargs:
        List of keyword arguments to be passed to the spark function to build
        the column.
        To support spark functions expecting column argument, col("x"),
        lit("3") and expr("x*2") can be provided.
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
    import pandas as pd
    from laktory import models

    df0 = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 3]}))

    node = models.SparkColumnNode(
        name="cosx",
        type="double",
        spark_func_name="cos",
        spark_func_args=["x"],
    )
    df = df0.withColumn("cosx", node.execute(df0))

    node = models.SparkColumnNode(
        name="xy",
        type="double",
        spark_func_name="coalesce",
        spark_func_args=["col('x')", "F.col('y')"],
        allow_missing_column_args=True,
    )
    df = df.withColumn("xy", node.execute(df))

    print(df.toPandas().to_string())
    '''
       x      cosx   xy
    0  1  0.540302  1.0
    1  2 -0.416147  2.0
    2  3 -0.989992  3.0
    '''
    ```
    """

    allow_missing_column_args: Union[bool, None] = False
    name: str
    spark_func_args: list[Union[Any, SparkFuncArg]] = []
    spark_func_kwargs: dict[str, Union[Any, SparkFuncArg]] = {}
    spark_func_name: Union[str, None] = None
    sql_expression: Union[str, None] = None
    type: str = "string"
    unit: Union[str, None] = None

    parse_args = parse_args
    parse_kwargs = parse_kwargs

    @field_validator("type")
    def check_type(cls, v: str) -> str:
        if "<" in v:
            return v
        else:
            if v not in SUPPORTED_TYPES:
                raise ValueError(
                    f"Type {v} is not supported. Select one of {SUPPORTED_TYPES}"
                )
        return v

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    def execute(
        self,
        df: DataFrame,
        udfs: list[Callable[[...], Column]] = None,
    ) -> Column:
        """
        Build Spark Column

        Parameters
        ----------
        df:
            Input DataFrame
        udfs:
            User-defined functions

        Returns
        -------
            Output column
        """
        import pyspark.sql.functions as F
        from pyspark.sql import Column
        from laktory.spark.dataframe import has_column
        from laktory.spark import functions as LF

        if udfs is None:
            udfs = []
        udfs = {f.__name__: f for f in udfs}

        # From SQL expression
        if self.sql_expression:
            logger.info(f"{self.name}[{self.type}] as `{self.sql_expression}`)")
            col = F.expr(self.sql_expression).alias(self.name)
            if self.type not in ["_any"]:
                col = col.cast(self.type)
            return col

        # From Spark Function
        func_name = self.spark_func_name
        if func_name is None:
            func_name = "coalesce"

        # Get from UDFs
        f = udfs.get(func_name, None)
        if f is None:
            # Get from built-in spark functions
            f = getattr(F, func_name, None)

            if f is None:
                # Get from laktory functions
                f = getattr(LF, func_name, None)

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
        logger.info(f"{self.name}[{self.type}] as {func_log}")

        # Build args
        args = []
        missing_column_names = []
        for i, _arg in enumerate(_args):
            parg = _arg.eval()
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
            kwargs[k] = _arg.eval()

        # Function call
        logger.info(f"{self.name}[{self.type}] as {func_name}({args}, {kwargs})")
        col = f(*args, **kwargs)

        # Type Casting
        if self.type not in ["_any"]:
            col = col.cast(self.type)

        return col
