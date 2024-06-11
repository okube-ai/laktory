from pydantic import field_validator
from pydantic import model_validator
from typing import Any
from typing import Union
from typing import Callable
import abc

from laktory._logger import get_logger
from laktory.constants import SUPPORTED_DATATYPES
from laktory.models.basemodel import BaseModel
from laktory.polars import PolarsDataFrame
from laktory.polars import PolarsExpr
from laktory.types import AnyDataFrame

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Classes                                                              #
# --------------------------------------------------------------------------- #


class BaseChainNodeFuncArg(BaseModel):
    value: Union[Any]

    @field_validator("value")
    def value_to_data_source(cls, v: Any) -> Any:
        """
        Data source can't be set as an expected value type as it would create
        a circular dependency. Instead, we check at validation if the value
        could be instantiated as a DataSource object.
        """
        from laktory.models.datasources import classes

        for c in classes:
            try:
                v = c(**v)
                break
            except:
                pass

        return v

    @abc.abstractmethod
    def eval(self):
        raise NotImplementedError()

    def signature(self):
        from laktory.models.datasources import PipelineNodeDataSource
        from laktory.models.datasources import FileDataSource
        from laktory.models.datasources import TableDataSource
        if isinstance(self.value, PipelineNodeDataSource):
            return f"node.{self.value.node_name}"
        elif isinstance(self.value, FileDataSource):
            return f"file {self.value.path}"
        elif isinstance(self.value, TableDataSource):
            return f"table {self.value.full_name}"
        return str(self.value)


class BaseChainNodeColumn(BaseModel):
    name: str
    type: str = "string"
    unit: Union[str, None] = None
    expr: Union[str, None] = None
    sql_expr: Union[str, None] = None

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

    @abc.abstractmethod
    def eval(self, udfs=None):
        raise NotImplementedError()

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


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class BaseChainNode(BaseModel):

    func_args: list[Union[Any]] = []
    func_kwargs: dict[str, Union[Any]] = {}
    func_name: Union[str, None] = None
    sql_expr: Union[str, None] = None
    with_column: Union[BaseChainNodeColumn, None] = None
    with_columns: Union[list[BaseChainNodeColumn], None] = []
    _parent: "BaseChain" = None
    _parsed_func_args: list = None
    _parsed_func_kwargs: dict = None

    @model_validator(mode="after")
    def selected_flow(self) -> Any:

        if len(self._with_columns) > 0:
            if self.func_name:
                raise ValueError(
                    "`func_name` should not be set when using `with_column`"
                )
            if self.sql_expr:
                raise ValueError(
                    "`sql_expr` should not be set when using `with_column`"
                )
        else:
            if self.func_name and self.sql_expr:
                raise ValueError(
                    "Only one of `func_name` and `sql_expr` should be set."
                )
            if not (self.func_name or self.sql_expr):
                raise ValueError(
                    "Either `func_name`, `sql_expr` or `with_column` should be set when using"
                )

        return self

    @property
    def _with_columns(self) -> list[BaseChainNodeColumn]:
        with_columns = [c for c in self.with_columns]
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

    @abc.abstractmethod
    def execute(
        self,
        df: AnyDataFrame,
        udfs: list[Callable[[...], Union[PolarsExpr, PolarsDataFrame]]] = None,
        return_col: bool = False,
    ) -> Union[AnyDataFrame]:
        raise NotImplementedError()
