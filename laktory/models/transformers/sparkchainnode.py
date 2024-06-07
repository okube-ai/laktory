from pydantic import field_validator
from pydantic import ValidationError
from typing import Any
from typing import Union
from typing import Callable

from laktory._logger import get_logger
from laktory.constants import SUPPORTED_DATATYPES
from laktory.models.basemodel import BaseModel
from laktory.models.transformers.sparkfuncarg import SparkFuncArg
from laktory.spark import SparkDataFrame
from laktory.spark import SparkColumn

from laktory.exceptions import MissingColumnError
from laktory.exceptions import MissingColumnsError

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class SparkChainNodeColumn(BaseModel):
    """
    Spark chain node column definition

    Attributes
    ----------
    name:
        Column name
    type:
        Column data type
    unit:
        Column units
    """

    name: str
    type: str = "string"
    unit: Union[str, None] = None

    @field_validator("type")
    def check_type(cls, v: str) -> str:
        if "<" in v:
            return v
        else:
            if v not in SUPPORTED_DATATYPES:
                raise ValueError(
                    f"Type {v} is not supported. Select one of {SUPPORTED_DATATYPES}"
                )
        return v


class SparkChainNode(BaseModel):
    """
    SparkChain node that output a dataframe upon execution. As a convenience,
    `column` can be specified to create a new column. In this case, the spark
    function or sql expression is expected to return a column instead of a
    dataframe. Each node is executed sequentially in the provided order. A node
    may also be another Spark Chain.

    Attributes
    ----------
    allow_missing_column_args:
        If `True`, spark func column arguments are allowed to be missing
        without raising an exception.
    column:
        Column definition. If not `None`, the spark function or sql expression
        is expected to return a column instead of a dataframe.
    spark_func_args:
        List of arguments to be passed to the spark function.
        To support spark functions expecting column argument, col("x"),
        lit("3") and expr("x*2") can be provided.
    spark_func_kwargs:
        List of keyword arguments to be passed to the spark function.
        To support spark functions expecting column argument, col("x"),
        lit("3") and expr("x*2") can be provided.
    spark_func_name:
        Name of the spark function to build the dataframe. If `column` is
        specified, the spark function should return a column instead. Mutually
         exclusive to `sql_expression`.
    sql_expression:
        SQL Expression using `{df}` to reference upstream dataframe and
        defining how to build the output dataframe. If `column` is
        specified, the sql expression should define a column instead. Mutually
         exclusive to `spark_func_name`

    Examples
    --------
    ```py
    import pandas as pd
    from laktory import models

    df0 = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 2, 3]}))

    node = models.SparkChainNode(
        column={
            "name": "cosx",
            "type": "double",
        },
        spark_func_name="cos",
        spark_func_args=["x"],
    )
    df = node.execute(df0)

    node = models.SparkChainNode(
        column={
            "name": "xy",
            "type": "double",
        },
        spark_func_name="coalesce",
        spark_func_args=["col('x')", "F.col('y')"],
        allow_missing_column_args=True,
    )
    df = node.execute(df)

    print(df.toPandas().to_string())
    '''
       x      cosx   xy
    0  1  0.540302  1.0
    1  2 -0.416147  2.0
    2  2 -0.416147  2.0
    3  3 -0.989992  3.0
    '''

    node = models.SparkChainNode(
        spark_func_name="drop_duplicates",
        spark_func_args=[["x"]],
    )
    df = node.execute(df0)

    print(df.toPandas().to_string())
    '''
       x
    0  1
    1  2
    2  3
    '''
    ```
    """

    allow_missing_column_args: Union[bool, None] = False
    column: Union[SparkChainNodeColumn, None] = None
    spark_func_args: list[Union[Any, SparkFuncArg]] = []
    spark_func_kwargs: dict[str, Union[Any, SparkFuncArg]] = {}
    spark_func_name: Union[str, None] = None
    sql_expression: Union[str, None] = None

    @field_validator("spark_func_args")
    def parse_args(cls, args: list[Union[Any, SparkFuncArg]]) -> list[SparkFuncArg]:
        _args = []
        for a in args:
            try:
                a = SparkFuncArg(**a)
            except (ValidationError, TypeError):
                pass

            if isinstance(a, SparkFuncArg):
                pass
            else:
                a = SparkFuncArg(value=a)
            _args += [a]
        return _args

    @field_validator("spark_func_kwargs")
    def parse_kwargs(
        cls, kwargs: dict[str, Union[str, SparkFuncArg]]
    ) -> dict[str, SparkFuncArg]:
        _kwargs = {}
        for k, a in kwargs.items():

            try:
                a = SparkFuncArg(**a)
            except (ValidationError, TypeError):
                pass

            if isinstance(a, SparkFuncArg):
                pass
            else:
                a = SparkFuncArg(value=a)

            _kwargs[k] = a
        return _kwargs

    @property
    def is_column(self):
        return self.column is not None

    @property
    def id(self):
        if self.is_column:
            return self.column.name
        return "df"

    def add_column(self, df, col):
        return df.withColumn(self.column.name, col)

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    def execute(
        self,
        df: SparkDataFrame,
        udfs: list[Callable[[...], Union[SparkColumn, SparkDataFrame]]] = None,
        return_col: bool = False,
    ) -> Union[SparkDataFrame, SparkColumn]:
        """
        Execute spark chain node

        Parameters
        ----------
        df:
            Input dataframe
        udfs:
            User-defined functions
        return_col
            If `True` and column specified, function returns `Column` object
            instead of dataframe.

        Returns
        -------
            Output dataframe
        """
        import pyspark.sql.functions as F
        from pyspark.sql.dataframe import DataFrame
        from pyspark.sql.connect.dataframe import DataFrame as DataFrameConnect
        from pyspark.sql import Column
        from laktory.spark.dataframe import has_column
        from laktory.spark import DATATYPES_MAP

        if udfs is None:
            udfs = []
        udfs = {f.__name__: f for f in udfs}

        # From SQL expression
        if self.sql_expression:
            if self.is_column:
                logger.info(
                    f"{self.column.name}[{self.column.type}] as `{self.sql_expression}`)"
                )
                col = F.expr(self.sql_expression).alias(self.column.name)
                if self.column.type not in ["_any"]:
                    col = col.cast(DATATYPES_MAP[self.column.type.lower()])
                if return_col:
                    return col
                return self.add_column(df, col)
            else:
                df = df.sparkSession.sql(self.sql_expression, df=df)
                return df

        # From Spark Function
        func_name = self.spark_func_name
        if self.spark_func_name is None:
            if self.is_column:
                func_name = "coalesce"
            else:
                raise ValueError(
                    "`spark_func_name` must be specified if `sql_expression` is not specified"
                )

        # Get from UDFs
        f = udfs.get(func_name, None)

        # Get from built-in spark and spark extension (including Laktory) functions
        input_df = True
        if f is None:
            if self.is_column:
                if "." in func_name:
                    vals = func_name.split(".")
                    f = getattr(getattr(F, vals[0]), vals[1], None)
                else:
                    f = getattr(F, func_name, None)
            else:
                if isinstance(df, DataFrameConnect):
                    if "." in func_name:
                        input_df = True
                        vals = func_name.split(".")
                        f = getattr(getattr(DataFrameConnect, vals[0]), vals[1], None)
                    else:
                        f = getattr(DataFrameConnect, func_name, None)
                else:
                    if "." in func_name:
                        input_df = False
                        vals = func_name.split(".")
                        f = getattr(getattr(df, vals[0]), vals[1], None)
                    else:
                        f = getattr(DataFrame, func_name, None)

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
        if self.is_column:
            logger.info(f"Column {self.id}[{self.column.type}] as {func_log}")
        else:
            logger.info(f"DataFrame {self.id} as {func_log}")

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
                    if not df.laktory.has_column(cname):
                        missing_column_names += [cname]
                        if not self.allow_missing_column_args:
                            logger.error(
                                f"Input column {cname} is missing. Abort building {self.id}"
                            )
                            raise MissingColumnError(cname)
                        else:
                            logger.warn(
                                f"Input column {cname} is missing for building {self.id}"
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
            kwargs[k] = _arg.eval(df.sparkSession)

        # Function call
        logger.info(f"{self.id} as {func_name}({args}, {kwargs})")

        if self.is_column:
            col = f(*args, **kwargs)
            if self.column.type not in ["_any"]:
                col = col.cast(DATATYPES_MAP[self.column.type.lower()])
            if return_col:
                return col
            df = self.add_column(df, col)
        else:
            if input_df:
                df = f(df, *args, **kwargs)
            else:
                df = f(*args, **kwargs)

        return df
