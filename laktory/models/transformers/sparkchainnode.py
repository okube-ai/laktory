from typing import Union
from typing import Callable
from typing import Any

from laktory._logger import get_logger
from laktory.models.transformers.basechainnode import BaseChainNode
from laktory.models.transformers.basechainnode import BaseChainNodeColumn
from laktory.models.transformers.basechainnode import BaseChainNodeFuncArg
from laktory.spark import SparkColumn
from laktory.spark import SparkDataFrame


logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Classes                                                              #
# --------------------------------------------------------------------------- #


class SparkChainNodeFuncArg(BaseChainNodeFuncArg):
    """
    Spark function argument

    Attributes
    ----------
    value:
        Value of the argument
    """

    value: Union[Any]

    def eval(self):
        from laktory.models.datasources.basedatasource import BaseDataSource

        v = self.value

        if isinstance(v, BaseDataSource):
            v = self.value.read()

        elif isinstance(v, str):

            # Imports required to evaluate expressions
            import pyspark.sql.functions as F
            from pyspark.sql.functions import lit
            from pyspark.sql.functions import col
            from pyspark.sql.functions import expr

            targets = ["lit(", "col(", "expr(", "F."]

            for f in targets:
                if f in v:
                    v = eval(v)
                    break

        return v


class SparkChainNodeColumn(BaseChainNodeColumn):
    """
    Chain node column definition

    Attributes
    ----------
    name:
        Column name
    type:
        Column data type
    unit:
        Column units
    expr:
        String representation of a polars expression
    sql_expr:
        SQL expression
    """

    name: str
    type: str = "string"
    unit: Union[str, None] = None
    expr: Union[str, None] = None
    sql_expr: Union[str, None] = None

    def eval(self, udfs=None):

        # Adding udfs to global variables
        if udfs is None:
            udfs = {}
        for k, v in udfs.items():
            globals()[k] = v

        # Imports required to evaluate expressions
        import pyspark.sql.functions as F
        from pyspark.sql.functions import col
        from pyspark.sql.functions import lit

        if self.sql_expr:
            return F.expr(self.sql_expr)

        expr = eval(self.expr)

        # Cleaning up global variables
        for k, v in udfs.items():
            del globals()[k]

        return expr


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class SparkChainNode(BaseChainNode):
    """
    PolarsChain node that output a dataframe upon execution. As a convenience,
    `with_column` argument can be specified to create a new column from a
    spark or sql expression. Each node is executed sequentially in the
    provided order. A node may also be another Spark Chain.

    Attributes
    ----------
    func_args:
        List of arguments to be passed to the spark function. If the function
        expects a spark column, its string representation can be provided
        with support for `col`, `lit`, `expr` and `F.`.
    func_kwargs:
        List of keyword arguments to be passed to the spark function. If the
        function expects a spark column, its string representation can be
        provided with support for `col`, `lit`, `expr` and `F.`.
    func_name:
        Name of the spark function to build the dataframe. Mutually
        exclusive to `sql_expr` and `with_column`.
    sql_expr:
        SQL Expression using `{df}` to reference upstream dataframe and
        defining how to build the output dataframe. Mutually exclusive to
        `func_name` and `with_column`.
    with_column:
        Syntactic sugar for adding a column. Mutually exclusive to `func_name`
        and `sql_expr`.
    with_columns:
        Syntactic sugar for adding columns. Mutually exclusive to `func_name`
        and `sql_expr`.

    Examples
    --------
    ```py
    import pandas as pd
    from laktory import models

    df0 = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 2, 3]}))

    node = models.SparkChainNode(
        with_column={
            "name": "cosx",
            "type": "double",
            "expr": "F.cos('x')",
        },
    )
    df = node.execute(df0)

    node = models.SparkChainNode(
        with_column={
            "name": "xy",
            "type": "double",
            "expr": "F.coalesce('x')",
        },
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
        func_name="drop_duplicates",
        func_args=[["x"]],
    )
    df = node.execute(df)

    print(df.toPandas().to_string())
    '''
       x      cosx   xy
    0  1  0.540302  1.0
    1  2 -0.416147  2.0
    2  3 -0.989992  3.0
    '''
    ```
    """

    func_args: list[Union[Any]] = []
    func_kwargs: dict[str, Union[Any]] = {}
    func_name: Union[str, None] = None
    sql_expr: Union[str, None] = None
    with_column: Union[SparkChainNodeColumn, None] = None
    with_columns: Union[list[SparkChainNodeColumn], None] = []
    _parent: "SparkChain" = None
    _parsed_func_args: list = None
    _parsed_func_kwargs: dict = None

    @property
    def parsed_func_args(self):
        if not self._parsed_func_args:
            self._parsed_func_args = [
                SparkChainNodeFuncArg(value=a) for a in self.func_args
            ]
        return self._parsed_func_args

    @property
    def parsed_func_kwargs(self):
        if not self._parsed_func_kwargs:
            self._parsed_func_kwargs = {
                k: SparkChainNodeFuncArg(value=v) for k, v in self.func_kwargs.items()
            }
        return self._parsed_func_kwargs

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

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    def execute(
        self,
        df: SparkDataFrame,
        udfs: list[Callable[[...], Union[SparkColumn, SparkDataFrame]]] = None,
    ) -> Union[SparkDataFrame]:
        """
        Execute spark chain node

        Parameters
        ----------
        df:
            Input dataframe
        udfs:
            User-defined functions

        Returns
        -------
            Output dataframe
        """
        import pyspark.sql.functions as F
        from pyspark.sql.dataframe import DataFrame
        from pyspark.sql.connect.dataframe import DataFrame as DataFrameConnect
        from pyspark.sql import Column
        from laktory.spark import DATATYPES_MAP

        if udfs is None:
            udfs = []
        udfs = {f.__name__: f for f in udfs}

        # Build Columns
        if self._with_columns:
            for column in self._with_columns:
                logger.info(
                    f"Building column {column.name} as {column.expr or column.sql_expr}"
                )
                df = df.withColumns(
                    {
                        column.name: column.eval(udfs=udfs).cast(
                            DATATYPES_MAP[column.type]
                        )
                    }
                )
            return df

        # From SQL expression
        if self.sql_expr:
            df = df.sparkSession.sql(self.sql_expr, df=df)
            return df

        # Get Function
        func_name = self.func_name
        if self.func_name is None:
            raise ValueError(
                "`func_name` must be specified if `sql_expr` is not specified"
            )

        # Get from UDFs
        f = udfs.get(func_name, None)

        # Get from built-in spark and spark extension (including Laktory) functions
        input_df = True
        if f is None:
            # Get function from namespace extension
            if "." in func_name:
                input_df = False
                vals = func_name.split(".")
                f = getattr(getattr(df, vals[0]), vals[1], None)
            else:
                f = getattr(type(df), func_name, None)

        if f is None:
            raise ValueError(f"Function {func_name} is not available")

        _args = self.parsed_func_args
        _kwargs = self.parsed_func_kwargs

        # Build log
        func_log = f"{func_name}("
        func_log += ",".join([a.signature() for a in _args])
        func_log += ",".join([f"{k}={a.signature()}" for k, a in _kwargs.items()])
        func_log += ")"
        logger.info(f"DataFrame {self.id} as {func_log}")

        # Build args
        args = []
        for i, _arg in enumerate(_args):
            args += [_arg.eval()]

        # Build kwargs
        kwargs = {}
        for k, _arg in _kwargs.items():
            kwargs[k] = _arg.eval()

        # Call function
        if input_df:
            df = f(df, *args, **kwargs)
        else:
            df = f(*args, **kwargs)

        return df
