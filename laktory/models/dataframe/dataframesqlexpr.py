from __future__ import annotations

# import abc
import re

# from typing import Callable
# from typing import Literal
from typing import Union

from pydantic import Field

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.pipeline.pipelinechild import PipelineChild
from laktory.typing import AnyFrame

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helper Classes                                                              #
# --------------------------------------------------------------------------- #


class DataFrameSQLExpr(BaseModel, PipelineChild):
    """
    Chain node SQL expression
    """

    sql_expr: str = Field(..., description="SQL Expression")
    # _data_sources: list[PipelineNodeDataSource] = None

    def parsed_expr(self, view=False) -> list[str]:
        raise NotImplementedError()
        # from laktory.models.datasources.pipelinenodedatasource import (
        #     PipelineNodeDataSource,
        # )
        # from laktory.models.datasources.tabledatasource import TableDataSource

        expr = self.sql_expr
        # TODO: REVIEW VIEW SUPPORT
        # if view:
        #     pl_node = self.parent_pipeline_node
        #
        #     if pl_node and pl_node.source:
        #         source = pl_node.source
        #         if isinstance(source, TableDataSource):
        #             full_name = source.full_name
        #         elif isinstance(source, PipelineNodeDataSource):
        #             full_name = source.sink_table_full_name
        #         else:
        #             raise ValueError(
        #                 "VIEW sink only supports Table or Pipeline Node with Table sink data sources"
        #             )
        #         expr = expr.replace("{df}", full_name)
        #
        #     pl = self.parent_pipeline
        #     if pl:
        #         from laktory.models.datasinks.tabledatasink import TableDataSink
        #
        #         pattern = r"\{nodes\.(.*?)\}"
        #         matches = re.findall(pattern, expr)
        #         for m in matches:
        #             if m not in pl.nodes_dict:
        #                 raise ValueError(
        #                     f"Node '{m}' is not available from pipeline '{pl.name}'"
        #                 )
        #             sink = pl.nodes_dict[m].primary_sink
        #             if not isinstance(sink, TableDataSink):
        #                 raise ValueError(
        #                     f"Node '{m}' used in view creation does not have a Table sink"
        #                 )
        #             expr = expr.replace("{nodes." + m + "}", sink.full_name)
        #
        #     return expr

        expr = expr.replace("{df}", "df")
        pattern = r"\{nodes\.(.*?)\}"
        matches = re.findall(pattern, expr)
        for m in matches:
            expr = expr.replace("{nodes." + m + "}", f"nodes__{m}")

        return expr.split(";")

    # @property
    # def upstream_node_names(self) -> list[str]:
    #     if self.sql_expr is None:
    #         return []
    #
    #     names = []
    #
    #     pattern = r"\{nodes\.(.*?)\}"
    #     matches = re.findall(pattern, self.sql_expr)
    #     for m in matches:
    #         names += [m]
    #
    #     return names

    # @property
    # def data_sources(self) -> list[PipelineNodeDataSource]:
    #     if self._data_sources is None:
    #         if self.sql_expr is None:
    #             return []
    #
    #         from laktory.models.datasources.pipelinenodedatasource import (
    #             PipelineNodeDataSource,
    #         )
    #
    #         sources = []
    #
    #         pattern = r"\{nodes\.(.*?)\}"
    #         matches = re.findall(pattern, self.sql_expr)
    #         for m in matches:
    #             sources += [PipelineNodeDataSource(node_name=m)]
    #
    #         self._data_sources = sources
    #
    #     return self._data_sources

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

        # From SQL expression
        logger.info(f"DataFrame as \n{self.sql_expr.strip()}")
        dfs = {"df": df}
        for k, v in named_dfs.items():
            dfs[k] = v

        dfs = {k: v.to_native() for k, v in dfs.items()}
        df0 = list(dfs.values())[0]

        if backend == DataFrameBackends.POLARS:
            import polars as pl

            #
            # kwargs = {"df": df}
            # for source in self.data_sources:
            #     kwargs[f"nodes__{source.node.name}"] = source.read()
            # return pl.SQLContext(frames=dfs).execute(";".join(self.parsed_expr()))
            return pl.SQLContext(frames=dfs).execute(self.sql_expr)

        elif backend == DataFrameBackends.PYSPARK:
            _spark = df0.sparkSession

            # # Get pipeline node if executed from pipeline
            # pipeline_node = self.parent_pipeline_node
            #
            # # Set df id (to avoid temp view with conflicting names)
            # df_id = "df"
            # if pipeline_node:
            #     df_id = f"df_{pipeline_node.name}"

            # df.createOrReplaceTempView(df_id)
            # for source in self.data_sources:
            #     _df = source.read(spark=_spark)
            #     _df.createOrReplaceTempView(f"nodes__{source.node.name}")

            # Create views
            for k, _df in dfs.items():
                _df.createOrReplaceTempView(k)

            # Run query
            _df = None
            for expr in self.sql_expr.split(";"):
                # for expr in self.parsed_expr():
                if expr.replace("\n", " ").strip() == "":
                    continue
                # _df = _spark.laktory.sql(expr)
                _df = _spark.sql(expr)
            if _df is None:
                raise ValueError(f"SQL Expression '{self.sql_expr}' is invalid")
            return _df

        else:
            raise NotImplementedError(f"Backend '{backend}' is not supported.")
