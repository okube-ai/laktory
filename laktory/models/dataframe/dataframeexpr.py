# import abc
import re
from typing import TYPE_CHECKING
from typing import Literal

# from typing import Callable
# from typing import Literal
import narwhals as nw
from pydantic import Field

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild
from laktory.typing import AnyFrame

logger = get_logger(__name__)

if TYPE_CHECKING:
    from laktory.models.datasources.pipelinenodedatasource import PipelineNodeDataSource


# --------------------------------------------------------------------------- #
# Helper Functions                                                            #
# --------------------------------------------------------------------------- #


def to_safe_name(name):
    return name.replace("{", "__").replace("}", "__").replace("nodes.", "nodes_")


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

    expr: str = Field(..., description="SQL Expression")
    type: Literal["SQL"] = Field(
        "SQL",
        description="Expression type. Only SQL is currently supported, but `DF` could be added in the future.",
    )
    _data_sources: list["PipelineNodeDataSource"] = None

    @property
    def upstream_node_names(self) -> list[str]:
        """Get all upstream nodes"""
        if self.expr is None:
            return []

        names = []

        pattern = r"\{nodes\.(.*?)\}"
        matches = re.findall(pattern, self.expr)
        for m in matches:
            names += [m]

        return names

    @property
    def data_sources(self):
        """Get all sources required by SQL Expression"""
        if self._data_sources is None:
            from laktory.models.datasources.pipelinenodedatasource import (
                PipelineNodeDataSource,
            )

            sources = []

            pattern = r"\{nodes\.(.*?)\}"
            matches = re.findall(pattern, self.expr)
            for m in matches:
                sources += [PipelineNodeDataSource(node_name=m)]

            self._data_sources = sources

        return self._data_sources

    def to_sql(self, references=None):
        from laktory.models.datasources.pipelinenodedatasource import (
            PipelineNodeDataSource,
        )
        from laktory.models.datasources.tabledatasource import TableDataSource

        if references is None:
            references = {}

        # Resolve Data Sources (references to other nodes)
        for s in self.data_sources:
            references["{nodes." + s.node.name + "}"] = s.sink_table_full_name

        # Add node data source
        pl_node = self.parent_pipeline_node
        if pl_node and pl_node.source:
            s = pl_node.source
            if isinstance(s, TableDataSource):
                full_name = s.full_name
            elif isinstance(s, PipelineNodeDataSource):
                full_name = s.sink_table_full_name
            else:
                raise ValueError(
                    "VIEW sink only supports Table or Pipeline Node with Table sink data sources"
                )
            references["{df}"] = full_name

        expr = self.expr
        for k, v in references.items():
            expr = expr.replace(k, v)

        return expr
        #
        # from laktory.models.datasources.tabledatasource import TableDataSource
        #
        # expr = self.sql_expr
        #
        # pl_node = self.parent_pipeline_node
        #
        # if pl_node and pl_node.source:
        #     source = pl_node.source
        #     if isinstance(source, TableDataSource):
        #         full_name = source.full_name
        #     elif isinstance(source, PipelineNodeDataSource):
        #         full_name = source.sink_table_full_name
        #     else:
        #         raise ValueError(
        #             "VIEW sink only supports Table or Pipeline Node with Table sink data sources"
        #         )
        #     expr = expr.replace("{df}", full_name)
        #
        # pl = self.parent_pipeline
        # if pl:
        #     from laktory.models.datasinks.tabledatasink import TableDataSink
        #
        #     pattern = r"\{nodes\.(.*?)\}"
        #     matches = re.findall(pattern, expr)
        #     for m in matches:
        #         if m not in pl.nodes_dict:
        #             raise ValueError(
        #                 f"Node '{m}' is not available from pipeline '{pl.name}'"
        #             )
        #         sink = pl.nodes_dict[m].primary_sink
        #         if not isinstance(sink, TableDataSink):
        #             raise ValueError(
        #                 f"Node '{m}' used in view creation does not have a Table sink"
        #             )
        #         expr = expr.replace("{nodes." + m + "}", sink.full_name)
        #
        # return expr

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

        # Read Data Sources
        for s in self.data_sources:
            dfs[f"nodes.{s.node.name}"] = s.read()

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
                _dfs[to_safe_name(_k)] = v

            expr = to_safe_name(self.expr)

            df = pl.SQLContext(frames=_dfs).execute(expr)
            return nw.from_native(df)

        elif backend == DataFrameBackends.PYSPARK:
            _spark = df0.sparkSession

            # Create views
            # TODO: Using parametrized queries would be ideal, but it is not compatible
            #       with older versions of spark or Delta Live Tables.
            # Because PySpark does not support view names with {}, we replaced them
            # with double underscores (__)
            for k, _df in dfs.items():
                _k = "{" + k + "}"
                _df.createOrReplaceTempView(to_safe_name(_k))

            # Run query
            _df = None
            for expr in self.expr.split(";"):
                if expr.replace("\n", " ").strip() == "":
                    continue
                _df = _spark.sql(to_safe_name(expr))
            if _df is None:
                raise ValueError(f"SQL Expression '{self.expr}' is invalid")
            return nw.from_native(_df)

        else:
            raise NotImplementedError(f"Backend '{backend}' is not supported.")
