from typing import Union
from typing import Callable
from typing import Any

from laktory._logger import get_logger
from laktory.models.transformers.basechainnode import BaseChainNode
from laktory.models.transformers.basechainnode import BaseChainNodeColumn
from laktory.models.transformers.basechainnode import BaseChainNodeFuncArg
from laktory.polars import PolarsDataFrame
from laktory.polars import PolarsExpr


logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Classes                                                              #
# --------------------------------------------------------------------------- #


class PolarsChainNodeFuncArg(BaseChainNodeFuncArg):
    """
    Base function argument

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
            import polars as pl
            from polars import col
            from polars import lit
            from polars import sql_expr

            targets = ["lit(", "col(", "sql_expr(", "pl."]

            for f in targets:
                if f in v:
                    v = eval(v)
                    break

        return v


class PolarsChainNodeColumn(BaseChainNodeColumn):
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
        import polars as pl
        import polars.functions as F
        from polars import col
        from polars import lit

        if self.sql_expr:
            return pl.Expr.laktory.sql_expr(self.sql_expr)

        expr = eval(self.expr)

        # Cleaning up global variables
        for k, v in udfs.items():
            del globals()[k]

        return expr


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class PolarsChainNode(BaseChainNode):
    """
    PolarsChain node that output a dataframe upon execution. As a convenience,
    `with_column` argument can be specified to create a new column from a
    polars or sql expression.

    Attributes
    ----------
    func_args:
        List of arguments to be passed to the polars function. If the function
        expects a polars expression, its string representation can be provided
        with support for `col`, `lit`, `sql_expr` and `pl.`.
    func_kwargs:
        List of keyword arguments to be passed to the polars function.If the
        function expects a polars expression, its string representation can be
        provided with support for `col`, `lit`, `sql_expr` and `pl.`.
    func_name:
        Name of the polars function to build the dataframe. Mutually
        exclusive to `sql_expr` and `with_column`.
    sql_expr:
        SQL Expression using `self` to reference upstream dataframe and
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
    from laktory import models
    import polars as pl

    df0 = pl.DataFrame({"x": [1, 2, 2, 3]})
    print(df0.glimpse(return_as_string=True))
    '''
    Rows: 4
    Columns: 1
    $ x <i64> 1, 2, 2, 3
    '''

    node = models.PolarsChainNode(
        with_column={"name": "cosx", "type": "double", "expr": "pl.col('x').cos()"},
    )
    df = node.execute(df0)

    node = models.PolarsChainNode(
        with_column={
            "name": "xy",
            "type": "double",
            "expr": "pl.coalesce('x')",
        },
    )
    df = node.execute(df)
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 4
    Columns: 3
    $ x    <i64> 1, 2, 2, 3
    $ cosx <f64> 0.5403023058681398, -0.4161468365471424, -0.4161468365471424, -0.9899924966004454
    $ xy   <f64> 1.0, 2.0, 2.0, 3.0
    '''

    node = models.PolarsChainNode(
        func_name="unique",
        func_args=[["x"]],
        func_kwargs={"maintain_order": True},
    )
    df = node.execute(df)
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 3
    Columns: 3
    $ x    <i64> 1, 2, 3
    $ cosx <f64> 0.5403023058681398, -0.4161468365471424, -0.9899924966004454
    $ xy   <f64> 1.0, 2.0, 3.0
    '''
    ```
    """

    func_args: list[Union[Any]] = []
    func_kwargs: dict[str, Union[Any]] = {}
    func_name: Union[str, None] = None
    sql_expr: Union[str, None] = None
    with_column: Union[PolarsChainNodeColumn, None] = None
    with_columns: Union[list[PolarsChainNodeColumn], None] = []
    _parent: "PolarsChain" = None
    _parsed_func_args: list = None
    _parsed_func_kwargs: dict = None

    @property
    def parsed_func_args(self):
        if not self._parsed_func_args:
            self._parsed_func_args = [
                PolarsChainNodeFuncArg(value=a) for a in self.func_args
            ]
        return self._parsed_func_args

    @property
    def parsed_func_kwargs(self):
        if not self._parsed_func_kwargs:
            self._parsed_func_kwargs = {
                k: PolarsChainNodeFuncArg(value=v) for k, v in self.func_kwargs.items()
            }
        return self._parsed_func_kwargs

    @property
    def user_dftype(self) -> Union[str, None]:
        """
        User-configured dataframe type directly from model or from parent.
        """
        return "POLARS"
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
        df: PolarsDataFrame,
        udfs: list[Callable[[...], Union[PolarsExpr, PolarsDataFrame]]] = None,
    ) -> Union[PolarsDataFrame]:
        """
        Execute polars chain node

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
        import polars.functions as F
        from polars import Expr
        from polars import DataFrame
        from laktory.polars.datatypes import DATATYPES_MAP

        if udfs is None:
            udfs = []
        udfs = {f.__name__: f for f in udfs}

        # Build Columns
        if self._with_columns:
            for column in self._with_columns:
                logger.info(
                    f"Building column {column.name} as {column.expr or column.sql_expr}"
                )
                df = df.with_columns(
                    **{
                        column.name: column.eval(udfs=udfs).cast(
                            DATATYPES_MAP[column.type]
                        )
                    }
                )
            return df

        # From SQL expression
        if self.sql_expr:
            df = df.sql(self.sql_expr)
            return df

        # Get Function
        func_name = self.func_name
        if self.func_name is None:
            raise ValueError(
                "`func_name` must be specified if `sql_expr` is not specified"
            )

        # Get from UDFs
        f = udfs.get(func_name, None)

        # Get from built-in polars and polars extension (including Laktory) functions
        input_df = True
        if f is None:
            # Get function from namespace extension
            if "." in func_name:
                input_df = False
                vals = func_name.split(".")
                f = getattr(getattr(df, vals[0]), vals[1], None)
            else:
                f = getattr(DataFrame, func_name, None)

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
