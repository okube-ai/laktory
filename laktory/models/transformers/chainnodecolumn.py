from pydantic import field_validator
from pydantic import model_validator
from pydantic import ValidationError
from typing import Any
from typing import Union
from typing import Callable
from typing import Literal

from laktory._logger import get_logger
from laktory.constants import SUPPORTED_DATATYPES
from laktory.models.basemodel import BaseModel
from laktory.models.transformers.chainnodefuncarg import ChainNodeFuncArg
from laktory.polars import PolarsDataFrame
from laktory.polars import PolarsExpr
from laktory.types import AnyDataFrame


logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class ChainNodeColumn(BaseModel):
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
    """

    name: str
    type: str = "string"
    unit: Union[str, None] = None
    expr: Union[str, None] = None
    sql_expr: Union[str, None] = None
    dataframe_type: Literal["SPARK", "POLARS"] = "SPARK"

    @model_validator(mode="after")
    def expression_valid(self) -> Any:
        if not (self.expr or self.sql_expr):
            raise ValueError("Either `expr` or `sql_expr` must be defined.")
        return self

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

    def eval(self, udfs=None):

        # Adding udfs to global variables
        if udfs is None:
            udfs = {}
        for k, v in udfs.items():
            globals()[k] = v

        if self.dataframe_type == "SPARK":

            # Imports required to evaluate expressions
            import pyspark.sql.functions as F
            from pyspark.sql.functions import col
            from pyspark.sql.functions import lit

            if self.sql_expr:
                return F.expr(self.sql_expr)

        elif self.dataframe_type == "POLARS":

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


class BaseChainNode(BaseModel):
    """
    PolarsChain node that output a dataframe upon execution. As a convenience,
    `column` can be specified to create a new column. In this case, the polars
    function is expected to return a column instead of a dataframe. Each node
    is executed sequentially in the provided order. A node may also be another
    Polars Chain.

    Attributes
    ----------
    func_args:
        List of arguments to be passed to the polars function.
        To support spark functions expecting column argument, col("x"),
        lit("3") and expr("x*2") can be provided.
    func_kwargs:
        List of keyword arguments to be passed to the polars function.
        To support polars functions expecting column argument, col("x"),
        lit("3") and expr("x*2") can be provided.
    func_name:
        Name of the polars function to build the dataframe. If `column` is
        specified, the polars function should return a column instead. Mutually
         exclusive to `sql_expression`.
    sql_expr:
        SQL Expression using `self` to reference upstream dataframe and
        defining how to build the output dataframe. If `column` is
        specified, the sql expression should define a column instead. Mutually
        exclusive to `spark_func_name`
    with_column:
        Column definition. If not `None`, the spark function or sql expression
        is expected to return a column instead of a dataframe.
    with_columns:
        Columns definition. If not `None`, the spark function or sql expression
        is expected to return a column instead of a dataframe.


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
        with_column={
            "name": "cosx",
            "type": "double",
            "expr": "pl.col('x').cos()",
        },
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

    func_args: list[Union[Any, ChainNodeFuncArg]] = []
    func_kwargs: dict[str, Union[Any, ChainNodeFuncArg]] = {}
    func_name: Union[str, None] = None
    sql_expr: Union[str, None] = None
    with_column: Union[ChainNodeColumn, None] = None
    with_columns: Union[list[ChainNodeColumn], None] = []

    @field_validator("func_args")
    def parse_args(cls, args: list[Union[Any, ChainNodeFuncArg]]) -> list[ChainNodeFuncArg]:
        _args = []
        for a in args:
            try:
                a = ChainNodeFuncArg(**a)
            except (ValidationError, TypeError):
                pass

            if isinstance(a, ChainNodeFuncArg):
                pass
            else:
                a = ChainNodeFuncArg(value=a)
            _args += [a]
        return _args

    @field_validator("func_kwargs")
    def parse_kwargs(
        cls, kwargs: dict[str, Union[str, ChainNodeFuncArg]]
    ) -> dict[str, ChainNodeFuncArg]:
        _kwargs = {}
        for k, a in kwargs.items():

            try:
                a = ChainNodeFuncArg(**a)
            except (ValidationError, TypeError):
                pass

            if isinstance(a, ChainNodeFuncArg):
                pass
            else:
                a = ChainNodeFuncArg(value=a)

            _kwargs[k] = a
        return _kwargs

    @property
    def _with_columns(self) -> list[ChainNodeColumn]:
        with_columns = self.with_columns
        if self.with_column:
            with_columns += [self.with_column]
        return with_columns

    @property
    def is_column(self):
        return len(self._with_columns) > 0

    @property
    def id(self):
        if self.is_column:
            return "-".join([c.name for c in self.with_columns])
        return "df"

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    def execute(
        self,
        df: AnyDataFrame,
        udfs: list[Callable[[...], Union[PolarsExpr, PolarsDataFrame]]] = None,
        return_col: bool = False,
    ) -> Union[AnyDataFrame, PolarsExpr]:
        """
        Execute polars chain node

        Parameters
        ----------
        df:
            Input dataframe
        udfs:
            User-defined functions
        return_col
            If `True` and column specified, function returns `Expr` object
            instead of dataframe.

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
                logger.info(f"Building column {column.name} as {column.expr or column.sql_expr}")
                df = df.with_columns(**{column.name: column.eval(udfs=udfs).cast(DATATYPES_MAP[column.type])})
            return df

        # From SQL expression
        if self.sql_expr:
            df = df.sql(self.sql_expr)
            return df

        # Get Function
        func_name = self.func_name
        if self.func_name is None:
            raise ValueError(
                "`func_name` must be specified if `sql_expression` is not specified"
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

        _args = self.func_args
        _kwargs = self.func_kwargs

        # Build log
        func_log = f"{func_name}("
        func_log += ",".join([a.signature() for a in _args])
        func_log += ",".join([f"{k}:{a.signature()}" for k, a in _kwargs.items()])
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
