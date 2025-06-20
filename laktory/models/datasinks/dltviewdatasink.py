from typing import Literal

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.datasinks.basedatasink import BaseDataSink

logger = get_logger(__name__)


class DLTViewDataSink(BaseDataSink):
    dlt_view_name: str | None = Field(..., description="DLT View name")
    type: Literal["DLT_VIEW"] = Field("DLT_VIEW", frozen=True, description="Sink Type")

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self) -> str:
        return self.dlt_view_name

    @property
    def dlt_name(self) -> str:
        return self.dlt_view_name

    @property
    def upstream_node_names(self) -> list[str]:
        """Pipeline node names required to write sink"""
        return []

    @property
    def data_sources(self):
        """Get all sources feeding the sink"""
        return []

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Writers                                                                 #
    # ----------------------------------------------------------------------- #

    def write(self, *args, **kwargs):
        # DLT View is created outside of Laktory. Only logging view name.
        logger.info(f"Creating DLT view {self.dlt_view_name}")

    # ----------------------------------------------------------------------- #
    # Purge                                                                   #
    # ----------------------------------------------------------------------- #

    def purge(self):
        """
        Delete sink data and checkpoints
        """
        return

    # ----------------------------------------------------------------------- #
    # Source                                                                  #
    # ----------------------------------------------------------------------- #

    def as_source(self, as_stream=None) -> None:
        """
        Generate a table data source with the same properties as the sink.

        Parameters
        ----------
        as_stream:
            If `True`, sink will be read as stream.

        Returns
        -------
        :
            Table Data Source
        """
        return None
        # source = TableDataSource(
        #     catalog_name=self.catalog_name,
        #     table_name=self.table_name,
        #     schema_name=self.schema_name,
        #     type=self.type,
        #     dataframe_backend=self.df_backend,
        # )
        #
        # if as_stream:
        #     source.as_stream = as_stream
        #
        # if self.dataframe_backend:
        #     source.dataframe_backend = self.dataframe_backend
        # source.parent = self.parent
        #
        # return source
