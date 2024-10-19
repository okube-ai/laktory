from __future__ import annotations
from pydantic import field_validator
from pydantic import model_validator
from typing import Any
from typing import Union
from typing import Callable
from typing import Literal
from typing import TYPE_CHECKING
import abc
import re

from laktory._logger import get_logger
from laktory.constants import SUPPORTED_DATATYPES
from laktory.constants import DEFAULT_DFTYPE
from laktory.models.basemodel import BaseModel
from laktory.models.dataframecolumnexpression import DataFrameColumnExpression
from laktory.polars import PolarsDataFrame
from laktory.polars import PolarsExpr
from laktory.types import AnyDataFrame

if TYPE_CHECKING:
    from laktory.models.datasources.basedatasource import BaseDataSource
    from laktory.models.datasources.pipelinenodedatasource import PipelineNodeDataSource


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


class ChainNodeColumn(BaseModel):
    expr: Union[str, DataFrameColumnExpression]
    name: str
    type: Union[str, None] = "string"
    unit: Union[str, None] = None

    @model_validator(mode="after")
    def parse_expr(self) -> Any:
        if isinstance(self.expr, str):
            self.expr = DataFrameColumnExpression(value=self.expr)
        return self

    @field_validator("type")
    def check_type(cls, v: str) -> str:

        if v is None or "<" in v:
            return v
        else:
            if v not in SUPPORTED_DATATYPES:
                raise ValueError(
                    f"Type {v} is not supported. Select one of {SUPPORTED_DATATYPES}"
                )
        return v

    def eval(self, udfs=None, dataframe_type=DEFAULT_DFTYPE):
        return self.expr.eval(udfs=udfs, dataframe_type=dataframe_type)


class BaseChainNodeSQLExpr(BaseModel):
    """
    Chain node SQL expression

    Attributes
    ----------
    expr:
        SQL expression
    """

    expr: str
    _node_data_sources: list[PipelineNodeDataSource] = None

    def parsed_expr(self, df_id="df"):
        return self.expr

    @property
    def node_data_sources(self) -> list[PipelineNodeDataSource]:

        if self.expr is None:
            return []

        if self._node_data_sources is None:

            from laktory.models.datasources.pipelinenodedatasource import (
                PipelineNodeDataSource,
            )

            sources = []

            pattern = r"\{nodes\.(.*?)\}"
            matches = re.findall(pattern, self.expr)
            for m in matches:
                sources += [PipelineNodeDataSource(node_name=m)]

            self._node_data_sources = sources

        return self._node_data_sources

    def eval(self, df, chain_node=None):
        raise NotImplementedError()


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class BaseChainNode(BaseModel):

    dataframe_type: Literal["SPARK", "POLARS", None] = "SPARK"
    func_args: list[Union[Any]] = []
    func_kwargs: dict[str, Union[Any]] = {}
    func_name: Union[str, None] = None
    sql_expr: Union[str, None] = None
    with_column: Union[ChainNodeColumn, None] = None
    with_columns: Union[list[ChainNodeColumn], None] = []
    _parent: "BaseChain" = None
    _parsed_func_args: list = None
    _parsed_func_kwargs: dict = None
    _parsed_sql_expr: BaseChainNodeSQLExpr = None

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
    def _with_columns(self) -> list[ChainNodeColumn]:
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

    @property
    def user_dftype(self):
        if "dataframe_type" in self.model_fields_set:
            return self.dataframe_type
        return None

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    def get_sources(self, cls=None) -> list[BaseDataSource]:
        """Get all sources feeding the Chain Node"""

        from laktory.models.datasources.basedatasource import BaseDataSource

        if cls is None:
            cls = BaseDataSource

        sources = []
        for a in self.parsed_func_args:
            if isinstance(a.value, cls):
                sources += [a.value]
        for a in self.parsed_func_kwargs.values():
            if isinstance(a.value, cls):
                sources += [a.value]

        if self.sql_expr:
            sources += self.parsed_sql_expr.node_data_sources

        return sources

    @abc.abstractmethod
    def execute(
        self,
        df: AnyDataFrame,
        udfs: list[Callable[[...], Union[PolarsExpr, PolarsDataFrame]]] = None,
        return_col: bool = False,
    ) -> Union[AnyDataFrame]:
        raise NotImplementedError()
