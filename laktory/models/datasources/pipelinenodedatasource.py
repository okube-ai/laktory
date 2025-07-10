from pydantic import Field

from laktory._logger import get_logger
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.typing import AnyFrame

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

    Examples
    ---------
    ```python
    from laktory import models

    brz = models.PipelineNode(
        name="brz_stock_prices",
        source={
            "path": "/Volumes/sources/landing/events/yahoo-finance/stock_price",
            "format": "JSON",
        },
        sinks=[
            {
                "path": "/Volumes/sources/landing/tables/brz_stock_prices",
                "format": "PARQUET",
            }
        ],
    )

    slv = models.PipelineNode(
        name="slv_stock_prices",
        source={"node_name": "brz_stock_prices"},
        sinks=[
            {
                "path": "/Volumes/sources/landing/tables/slv_stock_prices",
                "format": "PARQUET",
            }
        ],
    )

    pl = models.Pipeline(name="pl-stock-prices", nodes=[brz, slv])

    # pl.execute(spark=spark)
    ```
    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/sourcessinks/)
    """

    node_name: str = Field(..., description="Name of the upstream pipeline node")
    type: str = Field("PIPELINE_NODE", frozen=True)

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self) -> str:
        return self.node_name

    @property
    def node(self):
        pl = self.parent_pipeline

        if pl is None:
            raise ValueError(f"Source '{self.node_name}' is not attached to a pipeline")

        if self.node_name not in pl.sorted_node_names:
            raise ValueError(
                f"Node '{self.node_name}' does not exists in pipeline '{pl.name}'"
            )

        return pl.nodes_dict[self.node_name]

    @property
    def sink_table_full_name(self):
        from laktory.models.datasinks.tabledatasink import TableDataSink

        node = self.node
        if not node.primary_sink:
            raise ValueError(
                f"Source node '{self.node_name}' doest not have a sink defined"
            )
        if not isinstance(node.primary_sink, TableDataSink):
            raise ValueError(
                f"Source node '{self.node_name}' sink is not of type `TableDataSink`"
            )
        return node.primary_sink.full_name

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_spark(self) -> AnyFrame:
        stream_to_batch = not self.as_stream and self.node.source.as_stream
        is_dlt = False

        pl = self.parent_pipeline
        is_orchestrator_dlt = pl is not None and pl.is_orchestrator_dlt

        if is_orchestrator_dlt:
            from laktory import is_dlt_execute

            is_dlt = is_dlt_execute()

        # Reading from DLT
        if is_dlt:
            import dlt

            if self.as_stream:
                logger.info(f"Reading pipeline node {self._id} with DLT as stream")
                df = dlt.read_stream(self.node.primary_sink.dlt_name)
            else:
                logger.info(f"Reading pipeline node {self._id} with DLT as static")
                df = dlt.read(self.node.primary_sink.dlt_name)

        elif stream_to_batch or self.node.output_df is None:
            if self.node.has_sinks:
                logger.info(f"Reading pipeline node {self._id} from primary sink")
                df = self.node.primary_sink.read(as_stream=self.as_stream)
            else:
                logger.info("Executing parent pipeline node")
                self.node.execute()
                df = self.node.output_df

        elif self.node.output_df is not None:
            logger.info(f"Reading pipeline node {self._id} from output DataFrame")
            df = self.node.output_df

        else:
            raise ValueError(f"Pipeline Node {self._id} can't read DataFrame")

        return df

    def _read_polars(self) -> AnyFrame:
        # Read from node output DataFrame (if available)
        if self.node.output_df is not None:
            logger.info(f"Reading pipeline node {self._id} from output DataFrame")
            df = self.node.output_df

        # Read from node sink
        elif self.node.primary_sink:
            logger.info(f"Reading pipeline node {self._id} from sink")
            df = self.node.primary_sink.read(as_stream=self.as_stream)

        else:
            raise ValueError(f"Pipeline Node {self._id} can't read DataFrame")

        return df
