# import abc
import re
from typing import Literal

# from typing import Callable
# from typing import Literal
import narwhals as nw
from pydantic import Field
from pydantic import field_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild
from laktory.typing import AnyFrame

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Functions                                                            #
# --------------------------------------------------------------------------- #


def to_safe_expr(expr, df_names=None):
    if df_names is None:
        df_names = []
    df_names += ["df"]

    # {nodes.X} → __nodes_X___
    expr = re.sub(r"\{nodes\.(.*?)\}", r"__nodes_\1___", expr)

    # {sources.X} → __sources_X___
    expr = re.sub(r"\{sources\.(.*?)\}", r"__sources_\1___", expr)

    # Replace remaining bare {name} placeholders (e.g. {df}, legacy bare keys)
    for df_name in df_names:
        expr = expr.replace("{" + df_name + "}", f"__{df_name}__")

    return expr


_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def _iter_top_level_semicolons(expr: str):
    """Yield the index of every `;` in `expr` not inside a `--`/`/* */`
    comment or a quoted string/identifier literal (`'`, `"`, `` ` ``)."""
    in_line_comment = False
    in_block_comment = False
    quote_char = None
    i, n = 0, len(expr)
    while i < n:
        c = expr[i]
        nxt = expr[i + 1] if i + 1 < n else ""
        if in_line_comment:
            if c == "\n":
                in_line_comment = False
            i += 1
        elif in_block_comment:
            if c == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
        elif quote_char:
            if c == quote_char:
                if nxt == quote_char:  # doubled-quote escape
                    i += 2
                    continue
                quote_char = None
            i += 1
        elif c == "-" and nxt == "-":
            in_line_comment = True
            i += 2
        elif c == "/" and nxt == "*":
            in_block_comment = True
            i += 2
        elif c in ("'", '"', "`"):
            quote_char = c
            i += 1
        elif c == ";":
            yield i
            i += 1
        else:
            i += 1


def validate_single_statement(expr: str) -> None:
    """Raise if `expr` contains more than one SQL statement - a `;` outside
    of comments/literals that isn't simply the trailing terminator."""
    positions = list(_iter_top_level_semicolons(expr))
    if not positions:
        return

    tail = expr[positions[-1] + 1 :]
    tail = _LINE_COMMENT_RE.sub("", tail)
    tail = _BLOCK_COMMENT_RE.sub("", tail)
    if len(positions) > 1 or tail.strip():
        raise ValueError(
            "DataFrameExpr `expr` must be a single SQL statement. Found a "
            "statement-separating ';' (outside of comments and string "
            "literals) that is not just a trailing terminator."
        )


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #


class DataFrameExpr(BaseModel, PipelineChild):
    """
    A DataFrame expressed as a SQL statement.

    Examples
    --------
    ```py
    import polars as pl

    import laktory as lk

    df0 = pl.DataFrame(
        {
            "x": [1, 2, 3],
        }
    )

    expr = lk.models.DataFrameExpr(expr="SELECT x, 2*x AS y FROM {df}")
    df = expr.to_df(dfs={"df": df0}).collect()

    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |    | x | y |     |
    |    |---|---|     |
    |    | 1 | 2 |     |
    |    | 2 | 4 |     |
    |    | 3 | 6 |     |
    └──────────────────┘
    '''
    ```
    """

    expr: str = Field(
        ...,
        description=(
            "SQL Expression. Must be a single SQL statement - multiple "
            "`;`-separated statements are not supported, including a `;` "
            "appearing inside a `-- comment`."
        ),
    )
    type: Literal["SQL"] = Field(
        "SQL",
        description="Expression type. Only SQL is currently supported, but `DF` could be added in the future.",
    )

    @field_validator("expr")
    def check_single_statement(cls, v: str) -> str:
        validate_single_statement(v)
        return v

    @property
    def is_sql_expressible(self) -> bool:
        return True

    @property
    def upstream_node_names(self) -> list[str]:
        """Get all upstream nodes referenced in the SQL expression."""
        if self.expr is None:
            return []
        pattern = r"\{nodes\.(.*?)\}"
        return re.findall(pattern, self.expr)

    @property
    def data_sources(self):
        """Get PipelineNodeDataSource objects for each {nodes.X} reference in the SQL expression."""
        from laktory.models.datasources.pipelinenodedatasource import (
            PipelineNodeDataSource,
        )

        return [PipelineNodeDataSource(node_name=n) for n in self.upstream_node_names]

    def to_sql(self, references=None):
        from laktory.models.datasources.pipelinenodedatasource import (
            PipelineNodeDataSource,
        )
        from laktory.models.datasources.tabledatasource import TableDataSource

        if references is None:
            references = {}

        # Resolve {nodes.X} references via pipeline node registry
        pl = self.parent_pipeline
        pattern = r"\{nodes\.(.*?)\}"
        for node_name in re.findall(pattern, self.expr):
            if pl and node_name in pl.nodes_dict:
                upstream = pl.nodes_dict[node_name]
                if upstream.primary_sink:
                    references["{nodes." + node_name + "}"] = (
                        upstream.primary_sink.full_name
                    )

        # Resolve {sources.X} and backward-compat {df} to physical table names
        pl_node = self.parent_pipeline_node
        if pl_node:

            def _source_full_name(src):
                if isinstance(src, TableDataSource):
                    return src.full_name
                if isinstance(src, PipelineNodeDataSource):
                    return src.sink_table_full_name
                raise ValueError(
                    "VIEW sink only supports Table or Pipeline Node with Table sink data sources"
                )

            for source_key in re.findall(r"\{sources\.(.*?)\}", self.expr):
                src = next((s for s in pl_node.sources if s.name == source_key), None)
                if src:
                    references["{sources." + source_key + "}"] = _source_full_name(src)

            # {df} resolves to the primary (first) source - canonical for single unnamed sources
            if "{df}" in self.expr:
                df_source = next(iter(pl_node.sources), None)
                if df_source:
                    references["{df}"] = _source_full_name(df_source)

        expr = self.expr
        for k, v in references.items():
            expr = expr.replace(k, v)

        return expr

    def to_df(self, dfs: dict[str, AnyFrame]) -> AnyFrame:
        """
        Execute expression on provided DataFrame `dfs`.

        Parameters
        ----------
        dfs:
            Input dataframes

        Returns
        -------
            Output dataframe
        """

        # From SQL expression
        logger.info(f"DataFrame as \n{self.expr.strip()}")

        # Convert to Native
        dfs = {k: nw.from_native(v).to_native() for k, v in dfs.items()}
        df0 = list(dfs.values())[0]

        # Get Backend
        backend = DataFrameBackends.from_df(df0)

        if backend == DataFrameBackends.POLARS:
            import polars as pl

            #
            # kwargs = {"df": df}
            # for source in self.data_sources:
            #     kwargs[f"nodes__{source.node.name}"] = source.read()
            # return pl.SQLContext(frames=dfs).execute(";".join(self.parsed_expr()))

            # Because Polars don't support {} in frame names, we use
            # double underscores (__) instead
            _dfs = {}
            for k, v in dfs.items():
                _k = "{" + k + "}"
                _dfs[to_safe_expr(_k, df_names=[k])] = v

            expr = to_safe_expr(self.expr, df_names=list(dfs.keys()))

            df = pl.SQLContext(frames=_dfs).execute(expr)
            return nw.from_native(df)

        elif backend == DataFrameBackends.PYSPARK:
            _spark = df0.sparkSession

            from laktory import is_sdp_execute

            if is_sdp_execute():
                # Spark Connect (SDP): createOrReplaceTempView is forbidden inside
                # @dp.* decorated functions. Use spark.sql(**kwargs) instead -
                # PySpark creates SubqueryAlias plans internally without registering
                # temp views.
                #
                # {nodes.X} contains a dot which is not a valid Python kwarg key, so
                # use to_safe_expr(): {df} → __df__, {nodes.X} → __nodes_X___.
                # Escape all braces first so SQL patterns like {8,8} in regex literals
                # are not treated as format placeholders, then restore only our known
                # DataFrame placeholders as {safe_k}.
                sql_kwargs = {}
                query = self.expr.replace("{", "{{").replace("}", "}}")
                for k, v in dfs.items():
                    safe_k = to_safe_expr("{" + k + "}", df_names=[k])
                    sql_kwargs[safe_k] = v
                    query = query.replace("{{" + k + "}}", "{" + safe_k + "}")

                _df = _spark.sql(query, **sql_kwargs)
            else:
                # Local / LDP: use createOrReplaceTempView.
                # LDP monkey-patches spark.sql() and does not support **kwargs.
                # createOrReplaceTempView is safe outside Spark Connect.
                query = self.expr
                for k, v in dfs.items():
                    safe_k = to_safe_expr("{" + k + "}", df_names=[k])
                    query = query.replace("{" + k + "}", safe_k)
                    v.createOrReplaceTempView(safe_k)

                _df = _spark.sql(query)

            return nw.from_native(_df)

        else:
            raise NotImplementedError(f"Backend '{backend}' is not supported.")
