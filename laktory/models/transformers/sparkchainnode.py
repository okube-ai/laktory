from typing import Union
from typing import Callable
from typing import Any
from typing import Literal
import re

from laktory._logger import get_logger
from laktory.models.transformers.basechainnode import BaseChainNode
from laktory.models.transformers.basechainnode import ChainNodeColumn
from laktory.models.transformers.basechainnode import BaseChainNodeFuncArg
from laktory.models.transformers.basechainnode import BaseChainNodeSQLExpr
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

    def eval(self, spark=None):
        from laktory.models.datasources.basedatasource import BaseDataSource

        v = self.value

        if isinstance(v, BaseDataSource):
            v = self.value.read(spark=spark)

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


class SparkChainNodeSQLExpr(BaseChainNodeSQLExpr):
    """
    Chain node SQL expression

    Attributes
    ----------
    expr:
        SQL expression
    """

    def parsed_expr(self, df_id="df") -> list[str]:
        expr = self.expr.replace("{df}", df_id)
        pattern = r"\{nodes\.(.*?)\}"
        matches = re.findall(pattern, expr)
        for m in matches:
            expr = expr.replace("{nodes." + m + "}", f"nodes__{m}")

        return expr.split(";")

    def eval(self, df, chain_node=None):

        # We wanted to use parametrized queries to inject dataframes into the
        # query, but it does not seem to be mature enough to do so:
        # - as of Spark 3.5, spark connect does not support kwargs in .sql
        # - DLT does not currently support parametrized queries. Even if we
        #   bypass DLT custom sql function, the method org.apache.spark.sql.internal.CatalogImpl.dropTempView used in
        #   parametrized queries is not whitelisted and result in a py4j.security.Py4JSecurityException
        #
        # For now, we will create temp views manually.

        _spark = df.sparkSession

        # Get pipeline node if executed from pipeline
        pipeline_node = None
        if chain_node is not None:
            spark_chain = chain_node._parent
            if spark_chain is not None:
                pipeline_node = spark_chain._parent

        # Set df id (to avoid temp view with conflicting names)
        df_id = "df"
        if pipeline_node:
            df_id = f"df_{pipeline_node.name}"

        # Create views
        df.createOrReplaceTempView(df_id)
        for source in self.node_data_sources:
            _df = source.read(spark=_spark)
            _df.createOrReplaceTempView(f"nodes__{source.node.name}")

        # Run query
        _df = None
        for expr in self.parsed_expr(df_id):
            if expr.replace("\n", " ").strip() == "":
                continue
            _df = _spark.laktory.sql(expr)
        if _df is None:
            raise ValueError(f"SQL Expression '{self.expr}' is invalid")
        return _df

        # class Nodes:
        #     pass
        #
        # nodes = Nodes()
        # for source in self.node_data_sources:
        #     setattr(nodes, source.node.name, source.read(spark=df.sparkSession))
        # return df.sparkSession.laktory.sql(self.parsed_expr, df=df, nodes=nodes)


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
        `func_name` and `with_column`. Other pipeline nodes can also be
        referenced using {nodes.node_name}.
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

    dataframe_type: Literal["SPARK"] = "SPARK"
    func_args: list[Union[Any]] = []
    func_kwargs: dict[str, Union[Any]] = {}
    func_name: Union[str, None] = None
    sql_expr: Union[str, None] = None
    with_column: Union[ChainNodeColumn, None] = None
    with_columns: Union[list[ChainNodeColumn], None] = []
    _parent: "SparkChain" = None
    _parsed_func_args: list = None
    _parsed_func_kwargs: dict = None
    _parsed_sql_expr: SparkChainNodeSQLExpr = None

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
    def parsed_sql_expr(self):
        if self.sql_expr is None:
            return None
        if not self._parsed_sql_expr:
            self._parsed_sql_expr = SparkChainNodeSQLExpr(expr=self.sql_expr)
        return self._parsed_sql_expr

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
                _col = column.eval(udfs=udfs, dataframe_type="SPARK")
                if column.type:
                    _col = _col.cast(DATATYPES_MAP[column.type])
                df = df.withColumns({column.name: _col})
            return df

        # From SQL expression
        if self.sql_expr:
            logger.info(f"DataFrame {self.id} as \n{self.sql_expr.strip()}")
            return self.parsed_sql_expr.eval(df, chain_node=self)

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
            args += [_arg.eval(spark=df.sparkSession)]

        # Build kwargs
        kwargs = {}
        for k, _arg in _kwargs.items():
            kwargs[k] = _arg.eval(spark=df.sparkSession)

        # Call function
        if input_df:
            df = f(df, *args, **kwargs)
        else:
            df = f(*args, **kwargs)

        return df
