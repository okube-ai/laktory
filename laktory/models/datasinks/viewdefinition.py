from __future__ import annotations

import re
from typing import TYPE_CHECKING

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.pipeline.pipelinechild import PipelineChild

logger = get_logger(__name__)

if TYPE_CHECKING:
    from laktory.models.datasources.pipelinenodedatasource import PipelineNodeDataSource


# --------------------------------------------------------------------------- #
# Helper Classes                                                              #
# --------------------------------------------------------------------------- #


class ViewDefinition(BaseModel, PipelineChild):
    """
    A view definition.
    """

    sql_expr: str = Field(..., description="SQL Expression")
    _data_sources: list[PipelineNodeDataSource] = None

    def parsed(self):
        from laktory.models.datasources.tabledatasource import TableDataSource

        expr = self.sql_expr

        pl_node = self.parent_pipeline_node

        if pl_node and pl_node.source:
            source = pl_node.source
            if isinstance(source, TableDataSource):
                full_name = source.full_name
            elif isinstance(source, PipelineNodeDataSource):
                full_name = source.sink_table_full_name
            else:
                raise ValueError(
                    "VIEW sink only supports Table or Pipeline Node with Table sink data sources"
                )
            expr = expr.replace("{df}", full_name)

        pl = self.parent_pipeline
        if pl:
            from laktory.models.datasinks.tabledatasink import TableDataSink

            pattern = r"\{nodes\.(.*?)\}"
            matches = re.findall(pattern, expr)
            for m in matches:
                if m not in pl.nodes_dict:
                    raise ValueError(
                        f"Node '{m}' is not available from pipeline '{pl.name}'"
                    )
                sink = pl.nodes_dict[m].primary_sink
                if not isinstance(sink, TableDataSink):
                    raise ValueError(
                        f"Node '{m}' used in view creation does not have a Table sink"
                    )
                expr = expr.replace("{nodes." + m + "}", sink.full_name)

        return expr

    @property
    def data_sources(self):
        """Get all sources required by SQL Expression"""
        if self._data_sources is None:
            from laktory.models.datasources.pipelinenodedatasource import (
                PipelineNodeDataSource,
            )

            sources = []

            pattern = r"\{nodes\.(.*?)\}"
            matches = re.findall(pattern, self.sql_expr)
            for m in matches:
                sources += [PipelineNodeDataSource(node_name=m)]

            self._data_sources = sources

        return self._data_sources
