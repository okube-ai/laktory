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
    Data Source using an upstream pipeline node. Using a pipeline node data
    source defines the interdependencies between each node in a pipeline.
    Depending on the selected pipeline orchestrator and the context, a pipeline
    node data source might read the data from:

    - memory
    - upstream node sink
    - DLT table

    Attributes
    ----------
    node_name:
        Name of the upstream pipeline node

    Examples
    ---------
    ```python
    from laktory import models

    brz = models.PipelineNode(
        name="brz_stock_prices",
        source={"path": "/Volumes/sources/landing/events/yahoo-finance/stock_price"},
        sink={"path": "/Volumes/sources/landing/tables/brz_stock_prices"},
    )

    slv = models.PipelineNode(
        name="slv_stock_prices",
        source={"node_name": "brz_stock_prices"},
        sink={"path": "/Volumes/sources/landing/tables/slv_stock_prices"},
    )

    pl = models.Pipeline(name="pl-stock-prices", nodes=[brz, slv])

    # pl.execute(spark=spark)
    ```
    """

    node_name: Union[str, None]
    node: Any = Field(None, exclude=True)  # Add suggested type?
    # include_failed_expectations: bool = True  # TODO: Implement
    # include_passed_expectations: bool = True  # TODO: Implement

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self) -> str:
        return self.node_name

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_spark(self, spark) -> SparkDataFrame:

        # Reading from DLT
        if self.is_orchestrator_dlt:

            from laktory.dlt import read as dlt_read
            from laktory.dlt import read_stream as dlt_read_stream
            from laktory.dlt import is_debug

            if is_debug():
                df = None
                if self.node.output_df:
                    logger.info(
                        f"Reading pipeline node {self._id} from output DataFrame (DLT debug)"
                    )
                    df = self.node.output_df
                elif self.node.sink:
                    logger.info(
                        f"Reading pipeline node {self._id} from sink (DLT debug)"
                    )
                    df = self.node.sink.read(spark=spark, as_stream=self.as_stream)
                else:
                    logger.info(f"Can't read pipeline node {self._id} (DLT DEBUG)")

            else:
                if self.as_stream:
                    logger.info(f"Reading pipeline node {self._id} with DLT as stream")
                    df = dlt_read_stream(self.node.name)
                else:
                    logger.info(f"Reading pipeline node {self._id} with DLT as static")
                    df = dlt_read(self.node.name)

        # Read from node output DataFrame (if available)
        elif self.node.output_df is not None:
            logger.info(f"Reading pipeline node {self._id} from output DataFrame")
            df = self.node.output_df

        # Read from node sink
        elif self.node.sink:
            logger.info(f"Reading pipeline node {self._id} from sink")
            df = self.node.sink.read(spark=spark, as_stream=self.as_stream)

        else:
            raise ValueError(f"Pipeline Node {self._id} can't read DataFrame")

        return df

    def _read_polars(self) -> PolarsDataFrame:

        # Read from node output DataFrame (if available)
        if self.node.output_df is not None:
            logger.info(f"Reading pipeline node {self._id} from output DataFrame")
            df = self.node.output_df

        # Read from node sink
        elif self.node.sink:
            logger.info(f"Reading pipeline node {self._id} from sink")
            df = self.node.sink.read(as_stream=self.as_stream)

        else:
            raise ValueError(f"Pipeline Node {self._id} can't read DataFrame")

        return df
