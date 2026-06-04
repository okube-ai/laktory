# import abc
import re
from typing import Any

# from typing import Callable
# from typing import Literal
import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basechild import BaseChild
from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild
from laktory.typing import AnyFrame

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Classes                                                              #
# --------------------------------------------------------------------------- #


# Matches {sources.X}, {nodes.X}, or any bare {key} placeholder
_SOURCE_REF_PATTERN = re.compile(r"^\{(nodes\..+|[^{}]+)\}$")


class DataFrameMethodArg(BaseModel, PipelineChild):
    """
    DataFrame method argument: a scalar, a column expression, or a DataFrame
    reference string. DataFrame references use the same placeholder syntax as
    SQL expressions:

    - ``{df}`` - the current flowing DataFrame (primary source for the first transformer step)
    - ``{sources.name}`` - a named source declared on the pipeline node
    - ``{nodes.X}`` - the output of upstream pipeline node ``X``

    DataSources are **not** passed directly as objects; always use the string
    reference syntax above.
    """

    value: Any = Field(..., description="Function argument")

    @model_validator(mode="before")
    @classmethod
    def wrap_scalar(cls, data: Any) -> Any:
        # Dicts that already have a 'value' key are serialized DataFrameMethodArg
        # instances - pass them through so Pydantic parses them normally.
        # Everything else is wrapped into {"value": ...}.
        if isinstance(data, dict) and "value" in data:
            return data
        return {"value": data}

    def eval(self, backend: DataFrameBackends, named_dfs: dict | None = None):
        import narwhals as nw  # ensure nw is always bound before conditional imports below

        # TODO: Add supported for evaluating list or dict of strings.
        v = self.value

        if isinstance(v, str):
            # {sources.X}, {nodes.X}, or {key} → resolve from pre-loaded named_dfs
            m = _SOURCE_REF_PATTERN.match(v.strip())
            if m:
                key = m.group(1)
                if named_dfs is None or key not in named_dfs:
                    raise KeyError(
                        f"Method arg '{v}' references '{key}' which is not available. "
                        f"Available keys: {list(named_dfs or {})}"
                    )
                resolved = named_dfs[key]
                if not isinstance(resolved, (nw.DataFrame, nw.LazyFrame)):
                    resolved = nw.from_native(resolved)
                return (
                    resolved.to_native() if self.dataframe_api == "NATIVE" else resolved
                )

            # If the string contains a function call, evaluate it as a Python
            # expression with backend-appropriate names in scope. SyntaxError
            # means the string is a plain literal (e.g. "Walmart (Inc)") -
            # fall back to returning it as-is. All other errors propagate.
            if "(" in v:
                if self.dataframe_api == "NARWHALS":
                    import narwhals as nw  # noqa: F401
                    from narwhals import col  # noqa: F401
                    from narwhals import lit  # noqa: F401

                    from laktory.narwhals_ext.functions import sql_expr  # noqa: F401

                    v = v.replace("nw.sql_expr", "sql_expr")

                else:
                    if backend == DataFrameBackends.PYSPARK:
                        import pyspark.sql.functions as F  # noqa: F401
                        import pyspark.sql.types as T  # noqa: F401
                        from pyspark.sql.functions import col  # noqa: F401
                        from pyspark.sql.functions import expr  # noqa: F401
                        from pyspark.sql.functions import lit  # noqa: F401

                    elif backend == DataFrameBackends.POLARS:
                        import polars as pl  # noqa: F401
                        from polars import col  # noqa: F401
                        from polars import lit  # noqa: F401
                        from polars import sql_expr  # noqa: F401

                    else:
                        raise NotImplementedError()

                try:
                    v = eval(v)
                except SyntaxError:
                    logger.warning(
                        f"Could not evaluate '{v}' as a Python expression - treating as a plain string literal."
                    )

        return v

    def signature(self):
        return str(self.value)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class DataFrameMethod(BaseModel, PipelineChild):
    """
    Definition of a DataFrame method to be applied. Both native and Narwhals API are
    supported.

    Examples
    --------
    ```py
    import polars as pl

    import laktory as lk

    df0 = pl.DataFrame(
        {
            "x": [1.1, 2.2, 3.3],
        }
    )

    m1 = lk.models.DataFrameMethod(
        func_name="with_columns",
        func_kwargs={"xr": "nw.col('x').round()"},
        dataframe_api="NARWHALS",
    )
    df = m1.execute(df0)

    m2 = lk.models.DataFrameMethod(
        func_name="select", func_args=["pl.col('x').sqrt()"], dataframe_api="NATIVE"
    )
    df = m2.execute(df0)

    print(df.to_native())
    '''
    | x        |
    |----------|
    | 1.048809 |
    | 1.48324  |
    | 1.81659  |
    '''
    ```
    """

    func_args: list[DataFrameMethodArg] = Field(
        [],
        description="Arguments passed to method. Use ``{df}`` for the primary source, ``{sources.name}`` for a named source, or ``{nodes.X}`` for an upstream pipeline node output.",
    )
    func_kwargs: dict[str, DataFrameMethodArg] = Field(
        {},
        description="Keyword arguments passed to method. Use ``{df}`` for the primary source, ``{sources.name}`` for a named source, or ``{nodes.X}`` for an upstream pipeline node output.",
    )
    func_name: str = Field(
        ...,
        description=(
            "DataFrame method or attribute name (e.g. 'select', 'filter', 'dt.strftime'). "
            "Resolved as an attribute of the DataFrame object. "
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def check_direct_datasource_args(cls, data: Any) -> Any:
        # Migration guard: older pipeline configs passed DataSource objects directly
        # as func_args/func_kwargs values instead of using the string reference syntax
        # ({nodes.X} for pipeline nodes, {sources.<name>} for named sources).
        # This validator catches those patterns early and provides a clear migration
        # message rather than a confusing runtime error.
        # TODO: Remove once most users have migrated to the string reference syntax.
        if not isinstance(data, dict):
            return data

        _DATASOURCE_TYPES = frozenset(
            {"PIPELINE_NODE", "FILE", "DATAFRAME", "UNITY_CATALOG", "HIVE_METASTORE"}
        )

        def _check(v: Any, location: str) -> None:
            if not isinstance(v, dict):
                return
            if "node_name" in v:
                name = v["node_name"]
                raise ValueError(
                    f"{location}: DataSource objects must not be passed directly as "
                    f"function arguments. Use the string reference "
                    f"'{{nodes.{name}}}' instead."
                )
            if v.get("type") in _DATASOURCE_TYPES:
                raise ValueError(
                    f"{location}: DataSource objects must not be passed directly as "
                    f"function arguments. Declare the source on the pipeline node and "
                    f"reference it with '{{sources.<name>}}' string syntax instead."
                )

        for i, v in enumerate(data.get("func_args") or []):
            _check(v, f"func_args[{i}]")
        for k, v in (data.get("func_kwargs") or {}).items():
            _check(v, f"func_kwargs['{k}']")

        return data

    @model_validator(mode="after")
    def set_args(self) -> Any:
        # Pydantic does not trigger assign_parent_to_children() for mutations
        # to func_args/func_kwargs, so we set parent explicitly here.
        # Values may be VariableType strings (e.g. ${vars.X}) when a variable
        # reference is used — skip non-BaseChild entries.
        for v in self.func_args:
            if isinstance(v, BaseChild):
                v.parent = self
        for v in self.func_kwargs.values():
            if isinstance(v, BaseChild):
                v.parent = self
        return self

    @property
    def is_sql_expressible(self) -> bool:
        return False

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return [
            # "data_sources",
            "func_args",
            "func_kwargs",
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
    def data_sources(self) -> list:
        return []

    # ----------------------------------------------------------------------- #
    # Upstream Nodes                                                          #
    # ----------------------------------------------------------------------- #

    @property
    def upstream_node_names(self) -> list[str]:
        """Pipeline node names referenced via {nodes.X} in func_args / func_kwargs."""
        names = []
        for a in self.func_args:
            if not isinstance(a, DataFrameMethodArg):
                continue
            if isinstance(a.value, str):
                m = _SOURCE_REF_PATTERN.match(a.value.strip())
                if m and m.group(1).startswith("nodes."):
                    names.append(m.group(1)[len("nodes.") :])
        for a in self.func_kwargs.values():
            if not isinstance(a, DataFrameMethodArg):
                continue
            if isinstance(a.value, str):
                m = _SOURCE_REF_PATTERN.match(a.value.strip())
                if m and m.group(1).startswith("nodes."):
                    names.append(m.group(1)[len("nodes.") :])
        return names

    # ----------------------------------------------------------------------- #
    # Execution                                                               #
    # ----------------------------------------------------------------------- #

    def execute(self, df: AnyFrame, named_dfs: dict | None = None) -> AnyFrame:
        """
        Execute method on provided DataFrame `df`.

        Parameters
        ----------
        df:
            Input dataframe
        named_dfs:
            Pre-loaded named DataFrames available for ``{nodes.X}`` / ``{key}`` arg references.

        Returns
        -------
            Output dataframe
        """

        # Get and set Backend (required to evaluate arguments)
        backend = DataFrameBackends.from_df(df)
        self.dataframe_backend_ = backend

        # Convert to Narwhals
        if not isinstance(df, AnyFrame):
            df = nw.from_native(df)
        if self.dataframe_api == "NATIVE":
            df = df.to_native()

        # Get Function
        namespace = None
        func_name = self.func_name
        func_full_name = func_name
        if "." in func_name:
            namespace, func_name = func_name.split(".")
        df_as_input = False

        # Get from built-in narwhals and narwhals extension (including Laktory) functions
        f = None
        if f is None:
            # Get function from namespace extension
            if namespace:
                f = getattr(getattr(df, namespace), func_name, None)
            else:
                # getattr requires schema analysis - which SDP blocks. hasattr is safe
                if hasattr(type(df), func_name):
                    f = getattr(df, func_name)

        if f is None:
            df_type = type(df)
            raise ValueError(
                f"Function {func_full_name} is not available on dataframe of type {str(df_type)} with {self.dataframe_api} API"
            )

        _args = self.func_args
        _kwargs = self.func_kwargs

        # Build log
        func_log = f"df.{func_full_name}("
        func_log += ",".join([a.signature() for a in _args])
        func_log += ",".join([f"{k}={a.signature()}" for k, a in _kwargs.items()])
        func_log += f") with df type {type(df)}"
        logger.info(f"Applying {func_log}")

        # Build args
        args = []
        if df_as_input:
            args += [df]
        for i, _arg in enumerate(_args):
            args += [_arg.eval(backend=backend, named_dfs=named_dfs)]

        # Build kwargs
        kwargs = {}
        for k, _arg in _kwargs.items():
            kwargs[k] = _arg.eval(backend=backend, named_dfs=named_dfs)

        # Inject laktory_context if the function accepts it
        from laktory.models.laktorycontext import LaktoryContext
        from laktory.models.laktorycontext import _build_laktory_context_kwargs

        context = LaktoryContext(
            node=self.parent_pipeline_node,
            pipeline=self.parent_pipeline,
        )
        kwargs.update(_build_laktory_context_kwargs(f, context))

        # Call function
        df = f(*args, **kwargs)

        # Convert to narwhals when custom function don't return a Narwhals DataFrame
        if not isinstance(df, AnyFrame):
            df = nw.from_native(df)

        return df

    # def get_view_definition(self):
    #     return self._parsed_sql_expr.parsed_expr(view=True)


# TransformerFuncArg.model_rebuild()
