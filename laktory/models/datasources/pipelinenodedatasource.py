from typing import Union
from typing import Any
from pydantic import Field

from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.spark import SparkDataFrame
from laktory.polars import PolarsDataFrame
from laktory._logger import get_logger

logger = get_logger(__name__)


class PipelineNodeDataSource(BaseDataSource):
    """
    Data source using a data warehouse data table, generally used in the
    context of a data pipeline.

    Attributes
    ----------
    node_id:
        Id of the pipeline node to act as a data source

    Examples
    ---------
    ```python
    ```
    """

    node_id: Union[str, None]
    node: Any = Field(None, exclude=True)  # Add suggested type?

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self) -> str:
        return self.node_id

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_spark(self, spark) -> SparkDataFrame:

        # Reading from DLT
        if self.is_engine_dlt:

            from laktory.dlt import read as dlt_read
            from laktory.dlt import read_stream as dlt_read_stream
            from laktory.dlt import is_debug

            if is_debug():
                logger.info(f"Reading pipeline node {self._id} from sink (DLT debug)")
                df = None
                if self.node.sink:
                    df = self.node.sink.read(spark=spark, as_stream=self.as_stream)
            else:
                if self.as_stream:
                    logger.info(f"Reading pipeline node {self._id} with DLT as stream")
                    df = dlt_read_stream(self.node.id)
                else:
                    logger.info(f"Reading pipeline node {self._id} with DLT as static")
                    df = dlt_read(self.node.id)

        # Read from node output DataFrame (if available)
        elif self.node._output_df:
            logger.info(f"Reading pipeline node {self._id} from output DataFrame")
            df = self.node._output_df

        # Read from node sink
        elif self.node.sink:
            logger.info(f"Reading pipeline node {self._id} from sink")
            df = self.node.sink.read(spark=spark, as_stream=self.as_stream)

        else:
            raise ValueError(f"Pipeline Node {self.id} can't read DataFrame")

        return df

    def _read_polars(self) -> PolarsDataFrame:

        # Read from node output DataFrame (if available)
        if self.node._output_df:
            logger.info(f"Reading pipeline node {self._id} from output DataFrame")
            df = self.node._output_df

        # Read from node sink
        elif self.node.sink:
            logger.info(f"Reading pipeline node {self._id} from sink")
            df = self.node.sink.read(as_stream=self.as_stream)

        else:
            raise ValueError(f"Pipeline Node {self.id} can't read DataFrame")

        return df
