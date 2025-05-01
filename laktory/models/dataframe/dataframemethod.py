from __future__ import annotations

# import abc
from typing import Any

# from typing import Callable
# from typing import Literal
from typing import Union

import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.datasources import DataSourcesUnion
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.typing import AnyFrame

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Classes                                                              #
# --------------------------------------------------------------------------- #


class DataFrameMethodArg(BaseModel, PipelineChild):
    """
    DataFrame function argument expressed as a string or a serialized DataSource.
    """

    value: DataSourcesUnion | Any = Field(..., description="Function argument")

    def eval(self, backend: DataFrameBackends):
        from laktory.models.datasources.basedatasource import BaseDataSource

        v = self.value

        if isinstance(v, BaseDataSource):
            v = self.value.read()

        elif isinstance(v, str):
            # Imports required to evaluate expressions
            if self._dataframe_api == "NARWHALS":
                import narwhals as nw  # noqa: F401
                from narwhals import col  # noqa: F401
                from narwhals import lit  # noqa: F401

                from laktory.narwhals.expr import sql_expr  # noqa: F401

                targets = ["lit(", "col(", "nw.", "sql_expr"]

                # TODO: Review if we want to ducktype narwhals
                v = v.replace("nw.sql_expr", "sql_expr")

            else:
                from laktory.enums import DataFrameBackends

                if backend == DataFrameBackends.PYSPARK:
                    # Imports required to evaluate expressions
                    import pyspark.sql.functions as F  # noqa: F401
                    from pyspark.sql.functions import col  # noqa: F401
                    from pyspark.sql.functions import expr  # noqa: F401
                    from pyspark.sql.functions import lit  # noqa: F401

                    targets = ["lit(", "col(", "expr(", "F."]

                elif backend == DataFrameBackends.POLARS:
                    # Imports required to evaluate expressions
                    import polars as pl  # noqa: F401
                    from polars import col  # noqa: F401
                    from polars import lit  # noqa: F401
                    from polars import sql_expr  # noqa: F401

                    targets = ["lit(", "col(", "sql_expr(", "pl."]

                else:
                    raise NotImplementedError()

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
        return str(self.value)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class DataFrameMethod(BaseModel, PipelineChild):
    """
    DataFrame transformer node that output a dataframe upon execution. Each
    node is executed sequentially in the provided order.

    Attributes
    ----------
    func_args:
        List of arguments to be passed to the DataFrame function. If the
        function expects a column, it's string representation can be
        provided with support for `col`, `lit`, `sql_expr` and `nw.`.
    func_kwargs:
        List of keyword arguments to be passed to the DataFrame function. If
        the function expects a column, its string representation can be
        provided with support for `col`, `lit`, `expr` and `F.`.
    func_name:
        Name of the DataFrame function to build the output. Mutually
        exclusive to `sql_expr`.
    sql_expr:
        SQL Expression using `{df}` to reference upstream dataframe and
        defining how to build the output dataframe. Mutually exclusive to
        `func_name` and `with_column`. Other pipeline nodes can also be
        referenced using {nodes.node_name}.

    Examples
    --------
    ```py
    import polars as pl

    from laktory import models

    df0 = pl.DataFrame({"x": [1.2, 2.1, 2.0, 3.7]})

    node = models.DataFrameTransformerNode(
        func_name=with_columns, func_kwargs={"xr": "nw.col('x').round()"}
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

    args: list[Any] = Field([], description="")
    kwargs: dict[str, DataFrameMethodArg | Any] = {}
    name: str = None

    @model_validator(mode="after")
    def parse_args(self) -> Any:
        for k, v in self.kwargs.items():
            if not isinstance(v, DataFrameMethodArg):
                self.kwargs[k] = DataFrameMethodArg(value=v)

        for i, v in enumerate(self.args):
            if not isinstance(v, DataFrameMethodArg):
                self.args[i] = DataFrameMethodArg(value=v)

        self.update_children()

        return self

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def child_attribute_names(self):
        return [
            # "data_sources",
            "args",
            "kwargs",
            # "sql_expr",
        ]

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
    # ----------------------------------------------------------------------- #
    # Data Sources                                                            #
    # ----------------------------------------------------------------------- #

    @property
    def data_sources(self) -> list[DataSourcesUnion]:
        """Get all sources feeding the Chain Node"""

        sources = []
        # for a in self.parsed_func_args:
        #     if isinstance(a.value, BaseDataSource):
        #         sources += [a.value]
        # for a in self.parsed_func_kwargs.values():
        #     if isinstance(a.value, BaseDataSource):
        #         sources += [a.value]

        # if self.sql_expr:
        #     sources += self.parsed_sql_expr.data_sources

        return sources

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
        **named_dfs: dict[str, AnyFrame],
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

        # Get Backend
        backend = DataFrameBackends.from_df(df)
        #
        # from laktory.polars.datatypes import DATATYPES_MAP
        #
        # if udfs is None:
        #     udfs = []
        # udfs = {f.__name__: f for f in udfs}
        #

        # Convert to Narwhals
        if not isinstance(df, AnyFrame):
            df = nw.from_native(df)
        if self._dataframe_api == "NATIVE":
            df = df.to_native()

        # Get Function
        func_name = self.name
        if self.name is None:
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

        _args = self.args
        _kwargs = self.kwargs

        # Build log
        func_log = f"df.{func_name}("
        func_log += ",".join([a.signature() for a in _args])
        func_log += ",".join([f"{k}={a.signature()}" for k, a in _kwargs.items()])
        func_log += ")"
        logger.info(f"Applying {func_log}")

        # Build args
        args = []
        for i, _arg in enumerate(_args):
            args += [_arg.eval(backend=backend)]

        # Build kwargs
        kwargs = {}
        for k, _arg in _kwargs.items():
            kwargs[k] = _arg.eval(backend=backend)

        # Call function
        df = f(*args, **kwargs)

        return df

    # def get_view_definition(self):
    #     return self._parsed_sql_expr.parsed_expr(view=True)


# TransformerFuncArg.model_rebuild()
