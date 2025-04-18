from __future__ import annotations

# import abc
# import re
from typing import TYPE_CHECKING
from typing import Any

# from typing import Callable
# from typing import Literal
from typing import Union

import narwhals as nw

# from pydantic import field_validator
# from pydantic import model_validator
from laktory._logger import get_logger

# from laktory.constants import SUPPORTED_DATATYPES
from laktory.models.basemodel import BaseModel

# from laktory.polars import PolarsDataFrame
# from laktory.polars import PolarsExpr
# from laktory.typing import AnyDataFrame
from laktory.models.datasources import DataSourcesUnion

# from laktory.models.dataframecolumnexpression import DataFrameColumnExpression
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.typing import AnyFrame

if TYPE_CHECKING:
    pass
    # from laktory.models.datasources import DataSourcesUnion


logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Classes                                                              #
# --------------------------------------------------------------------------- #


class TransformerFuncArg(BaseModel, PipelineChild):
    """
    Base function argument

    Attributes
    ----------
    value:
        Value of the argument
    """

    value: DataSourcesUnion | Any
    #
    # @field_validator("value")
    # def value_to_data_source(cls, v: Any) -> Any:
    #     """
    #     Data source can't be set as an expected value type as it would create
    #     a circular dependency. Instead, we check at validation if the value
    #     could be instantiated as a DataSource object.
    #     """
    #     from laktory.models.datasources import classes
    #
    #     for c in classes:
    #         try:
    #             v = c(**v)
    #             break
    #         except:  # noqa: E722
    #             pass
    #
    #     return v

    def eval(self):
        from laktory.models.datasources.basedatasource import BaseDataSource

        v = self.value

        if isinstance(v, BaseDataSource):
            v = self.value.read()

        elif isinstance(v, str):
            # Imports required to evaluate expressions
            import narwhals as nw  # noqa: F401
            from narwhals import col  # noqa: F401
            from narwhals import lit  # noqa: F401
            # from polars import sql_expr  # noqa: F401

            targets = ["lit(", "col(", "nw."]
            # TODO: add spport for sql_expr(

            for f in targets:
                if f in v:
                    v = eval(v)
                    break

        return v

    def signature(self):
        from laktory.models.datasources import DataFrameDataSource
        from laktory.models.datasources import FileDataSource
        from laktory.models.datasources import PipelineNodeDataSource
        from laktory.models.datasources import TableDataSource

        if isinstance(self.value, DataFrameDataSource):
            return f"{self.value.df}"
        elif isinstance(self.value, PipelineNodeDataSource):
            return f"node.{self.value.node_name}"
        elif isinstance(self.value, FileDataSource):
            return f"file {self.value.path}"
        elif isinstance(self.value, TableDataSource):
            return f"table {self.value.full_name}"
        # else:
        #     print("value type", type(self.value))
        #     # print("GOT DATAFRAME111111111")
        return str(self.value)


# class BaseChainNodeFuncArg(BaseModel, PipelineChild):
#     value: Union[Any]
#
#     @field_validator("value")
#     def value_to_data_source(cls, v: Any) -> Any:
#         """
#         Data source can't be set as an expected value type as it would create
#         a circular dependency. Instead, we check at validation if the value
#         could be instantiated as a DataSource object.
#         """
#         from laktory.models.datasources import classes
#
#         for c in classes:
#             try:
#                 v = c(**v)
#                 break
#             except:  # noqa: E722
#                 pass
#
#         return v
#
#     @abc.abstractmethod
#     def eval(self):
#         raise NotImplementedError()
#
#     def signature(self):
#         from laktory.models.datasources import FileDataSource
#         from laktory.models.datasources import PipelineNodeDataSource
#         from laktory.models.datasources import TableDataSource
#
#         if isinstance(self.value, PipelineNodeDataSource):
#             return f"node.{self.value.node_name}"
#         elif isinstance(self.value, FileDataSource):
#             return f"file {self.value.path}"
#         elif isinstance(self.value, TableDataSource):
#             return f"table {self.value.full_name}"
#         return str(self.value)
#
#
# class ChainNodeColumn(BaseModel, PipelineChild):
#     expr: Union[str, DataFrameColumnExpression]
#     name: str
#     type: Union[str, None] = "string"
#     unit: Union[str, None] = None
#
#     @model_validator(mode="after")
#     def parse_expr(self) -> Any:
#         if isinstance(self.expr, str):
#             self.expr = DataFrameColumnExpression(value=self.expr)
#         return self
#
#     @field_validator("type")
#     def check_type(cls, v: str) -> str:
#         if v is None or "<" in v:
#             return v
#         else:
#             if v not in SUPPORTED_DATATYPES:
#                 raise ValueError(
#                     f"Type {v} is not supported. Select one of {SUPPORTED_DATATYPES}"
#                 )
#         return v
#
#     def eval(self, udfs=None, dataframe_backend=None):
#         return self.expr.eval(udfs=udfs, dataframe_backend=dataframe_backend)
#
#
# class BaseChainNodeSQLExpr(BaseModel, PipelineChild):
#     """
#     Chain node SQL expression
#
#     Attributes
#     ----------
#     expr:
#         SQL expression
#     """
#
#     expr: str
#     _data_sources: list[PipelineNodeDataSource] = None
#
#     def parsed_expr(self, df_id="df", view=False) -> list[str]:
#         from laktory.models.datasources.pipelinenodedatasource import (
#             PipelineNodeDataSource,
#         )
#         from laktory.models.datasources.tabledatasource import TableDataSource
#
#         expr = self.expr
#         if view:
#             pl_node = self.parent_pipeline_node
#
#             if pl_node and pl_node.source:
#                 source = pl_node.source
#                 if isinstance(source, TableDataSource):
#                     full_name = source.full_name
#                 elif isinstance(source, PipelineNodeDataSource):
#                     full_name = source.sink_table_full_name
#                 else:
#                     raise ValueError(
#                         "VIEW sink only supports Table or Pipeline Node with Table sink data sources"
#                     )
#                 expr = expr.replace("{df}", full_name)
#
#             pl = self.parent_pipeline
#             if pl:
#                 from laktory.models.datasinks.tabledatasink import TableDataSink
#
#                 pattern = r"\{nodes\.(.*?)\}"
#                 matches = re.findall(pattern, expr)
#                 for m in matches:
#                     if m not in pl.nodes_dict:
#                         raise ValueError(
#                             f"Node '{m}' is not available from pipeline '{pl.name}'"
#                         )
#                     sink = pl.nodes_dict[m].primary_sink
#                     if not isinstance(sink, TableDataSink):
#                         raise ValueError(
#                             f"Node '{m}' used in view creation does not have a Table sink"
#                         )
#                     expr = expr.replace("{nodes." + m + "}", sink.full_name)
#
#             return expr
#
#         expr = expr.replace("{df}", df_id)
#         pattern = r"\{nodes\.(.*?)\}"
#         matches = re.findall(pattern, expr)
#         for m in matches:
#             expr = expr.replace("{nodes." + m + "}", f"nodes__{m}")
#
#         return expr.split(";")
#
#     @property
#     def upstream_node_names(self) -> list[str]:
#         if self.expr is None:
#             return []
#
#         names = []
#
#         pattern = r"\{nodes\.(.*?)\}"
#         matches = re.findall(pattern, self.expr)
#         for m in matches:
#             names += [m]
#
#         return names
#
#     @property
#     def data_sources(self) -> list[PipelineNodeDataSource]:
#         if self._data_sources is None:
#             if self.expr is None:
#                 return []
#
#             from laktory.models.datasources.pipelinenodedatasource import (
#                 PipelineNodeDataSource,
#             )
#
#             sources = []
#
#             pattern = r"\{nodes\.(.*?)\}"
#             matches = re.findall(pattern, self.expr)
#             for m in matches:
#                 sources += [PipelineNodeDataSource(node_name=m)]
#
#             self._data_sources = sources
#
#         return self._data_sources
#
#     def eval(self, df):
#         raise NotImplementedError()
#

# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class DataFrameTransformerNode(BaseModel, PipelineChild):
    # dataframe_backend: Literal["SPARK", "POLARS", None] = None
    func_args: list[Any] = []
    func_kwargs: dict[str, Any] = {}
    func_name: str = None
    # sql_expr: Union[str, None] = None
    # with_column: Union[ChainNodeColumn, None] = None
    # with_columns: Union[list[ChainNodeColumn], None] = []
    _parsed_func_args: list = None
    _parsed_func_kwargs: dict = None
    # _parsed_sql_expr: BaseChainNodeSQLExpr = None
    #
    # @model_validator(mode="after")
    # def selected_flow(self) -> Any:
    #     if len(self._with_columns) > 0:
    #         if self.func_name:
    #             raise ValueError(
    #                 "`func_name` should not be set when using `with_column`"
    #             )
    #         if self.sql_expr:
    #             raise ValueError(
    #                 "`sql_expr` should not be set when using `with_column`"
    #             )
    #     else:
    #         if self.func_name and self.sql_expr:
    #             raise ValueError(
    #                 "Only one of `func_name` and `sql_expr` should be set."
    #             )
    #         if not (self.func_name or self.sql_expr):
    #             raise ValueError(
    #                 "Either `func_name`, `sql_expr` or `with_column` should be set when using"
    #             )
    #
    #     return self
    #

    # -------------------------------------------------------------------------------- #
    # Func Args                                                                        #
    # -------------------------------------------------------------------------------- #

    @property
    def parsed_func_args(self):
        if not self._parsed_func_args:
            self._parsed_func_args = [
                TransformerFuncArg(value=a) for a in self.func_args
            ]
        return self._parsed_func_args

    @property
    def parsed_func_kwargs(self):
        if not self._parsed_func_kwargs:
            self._parsed_func_kwargs = {
                k: TransformerFuncArg(value=v) for k, v in self.func_kwargs.items()
            }
        return self._parsed_func_kwargs

    # # ----------------------------------------------------------------------- #
    # # Id                                                                      #
    # # ----------------------------------------------------------------------- #
    #
    # @property
    # def id(self):
    #     if self.is_column:
    #         return "-".join([c.name for c in self.with_columns])
    #     return "df"
    #
    # # ----------------------------------------------------------------------- #
    # # Children                                                                #
    # # ----------------------------------------------------------------------- #
    #
    # @property
    # def child_attribute_names(self):
    #     return [
    #         "data_sources",
    #         "_parsed_func_args",
    #         "_parsed_func_kwargs",
    #         "_parsed_sql_expr",
    #     ]
    #
    # # ----------------------------------------------------------------------- #
    # # Columns Creation                                                        #
    # # ----------------------------------------------------------------------- #
    #
    # @property
    # def _with_columns(self) -> list[ChainNodeColumn]:
    #     with_columns = [c for c in self.with_columns]
    #     if self.with_column:
    #         with_columns += [self.with_column]
    #     return with_columns
    #
    # @property
    # def is_column(self):
    #     return len(self._with_columns) > 0
    #
    # # ----------------------------------------------------------------------- #
    # # Data Sources                                                            #
    # # ----------------------------------------------------------------------- #
    #
    # @property
    # def data_sources(self) -> list[BaseDataSource]:
    #     """Get all sources feeding the Chain Node"""
    #
    #     from laktory.models.datasources.basedatasource import BaseDataSource
    #
    #     sources = []
    #     for a in self.parsed_func_args:
    #         if isinstance(a.value, BaseDataSource):
    #             sources += [a.value]
    #     for a in self.parsed_func_kwargs.values():
    #         if isinstance(a.value, BaseDataSource):
    #             sources += [a.value]
    #
    #     if self.sql_expr:
    #         sources += self.parsed_sql_expr.data_sources
    #
    #     return sources
    #
    # # ----------------------------------------------------------------------- #
    # # Upstream Nodes                                                          #
    # # ----------------------------------------------------------------------- #
    #
    # @property
    # def upstream_node_names(self) -> list[str]:
    #     """Pipeline node names required to apply transformer node."""
    #
    #     from laktory.models.datasources.pipelinenodedatasource import (
    #         PipelineNodeDataSource,
    #     )
    #
    #     names = []
    #     for a in self.parsed_func_args:
    #         if isinstance(a.value, PipelineNodeDataSource):
    #             names += [a.value.node_name]
    #     for a in self.parsed_func_kwargs.values():
    #         if isinstance(a.value, PipelineNodeDataSource):
    #             names += [a.value.node_name]
    #
    #     if self.sql_expr:
    #         names += self.parsed_sql_expr.upstream_node_names
    #
    #     return names
    #
    # ----------------------------------------------------------------------- #
    # Execution                                                               #
    # ----------------------------------------------------------------------- #

    def execute(
        self,
        df: AnyFrame,
        # udfs: list[Callable[[...], Union[PolarsExpr, PolarsDataFrame]]] = None,
    ) -> Union[AnyFrame]:
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
        # from polars import DataFrame
        #
        # from laktory.polars.datatypes import DATATYPES_MAP
        #
        # if udfs is None:
        #     udfs = []
        # udfs = {f.__name__: f for f in udfs}
        #
        # # Build Columns
        # if self._with_columns:
        #     for column in self._with_columns:
        #         logger.info(
        #             f"Building column {column.name} as {column.expr or column.sql_expr}"
        #         )
        #         _col = column.eval(udfs=udfs, dataframe_backend="POLARS")
        #         if column.type:
        #             _col = _col.cast(DATATYPES_MAP[column.type])
        #         df = df.with_columns(**{column.name: _col})
        #     return df
        #
        # # From SQL expression
        # if self.sql_expr:
        #     logger.info(f"DataFrame {self.id} as \n{self.sql_expr.strip()}")
        #     return self.parsed_sql_expr.eval(df)

        # Convert to Narwhals
        if not isinstance(df, AnyFrame):
            df = nw.from_native(df)

        # Get Function
        func_name = self.func_name
        if self.func_name is None:
            raise ValueError(
                "`func_name` must be specified if `sql_expr` is not specified"
            )

        # Get from UDFs
        # f = udfs.get(func_name, None)
        f = None

        # Get from built-in narwhals and narwhals extension (including Laktory) functions
        if f is None:
            # Get function from namespace extension
            if "." in func_name:
                vals = func_name.split(".")
                f = getattr(getattr(df, vals[0]), vals[1], None)
            else:
                f = getattr(df, func_name, None)

        if f is None:
            raise ValueError(f"Function {func_name} is not available")

        _args = self.parsed_func_args
        _kwargs = self.parsed_func_kwargs

        # Build log
        func_log = f"df.{func_name}("
        func_log += ",".join([a.signature() for a in _args])
        func_log += ",".join([f"{k}={a.signature()}" for k, a in _kwargs.items()])
        func_log += ")"
        logger.info(f"Applying {func_log}")

        # Build args
        # args = [df]
        args = []
        for i, _arg in enumerate(_args):
            args += [_arg.eval()]

        # Build kwargs
        kwargs = {}
        for k, _arg in _kwargs.items():
            kwargs[k] = _arg.eval()

        # Call function
        df = f(*args, **kwargs)

        return df

    # @abc.abstractmethod
    # def execute(
    #     self,
    #     df: AnyDataFrame,
    #     udfs: list[Callable[[...], Union[PolarsExpr, PolarsDataFrame]]] = None,
    #     return_col: bool = False,
    # ) -> Union[AnyDataFrame]:
    #     raise NotImplementedError()
    #
    # def get_view_definition(self):
    #     return self._parsed_sql_expr.parsed_expr(view=True)


# TransformerFuncArg.model_rebuild()
