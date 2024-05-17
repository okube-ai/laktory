from typing import Union
from typing import Any

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
    df:
        DataFrame output from the pipeline node

    Examples
    ---------
    ```python
    ```
    """

    node_id: Union[str, None]
    node: Any = None  # Add suggested type?
    _df: Any = None

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
        logger.info(f"Reading {self._id} from pipeline node")
        return self.node._df

    def _read_polars(self) -> PolarsDataFrame:
        logger.info(f"Reading {self._id} from pipeline node")
        return self.node._df
