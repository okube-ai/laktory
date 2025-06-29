from typing import Literal

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.datasinks.basedatasink import BaseDataSink

logger = get_logger(__name__)


class PipelineViewDataSink(BaseDataSink):
    """
    Data sink writing to a Declarative Pipeline (formerly Delta Live Tables) view. This
    view is virtual and does not materialize data.

    Examples
    ---------
    ```python tag:skip-run
    from laktory import models

    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])

    sink = models.PipelineViewDataSink(
        pipeline_view_name="my_view",
    )
    sink.write(df)
    ```
    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/sourcessinks/)
    """

    pipeline_view_name: str | None = Field(..., description="Pipeline View name")
    type: Literal["PIPELINE_VIEW"] = Field(
        "PIPELINE_VIEW", frozen=True, description="Sink Type"
    )

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self) -> str:
        return self.pipeline_view_name

    @property
    def dlt_name(self) -> str:
        return self.pipeline_view_name

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
        logger.info(f"Creating DLT view {self.pipeline_view_name}")

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
        raise NotImplementedError()
