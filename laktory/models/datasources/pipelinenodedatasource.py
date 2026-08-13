from typing import Any
from typing import Literal

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.models.readerwritermethod import ReaderWriterMethod
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
    - LDP table

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
    reader_kwargs: dict[str, Any] = Field(
        {},
        description="""
        Keyword arguments passed directly to dataframe backend reader. Passed to `.options()` method when using PySpark.
        """,
    )
    reader_methods: list[ReaderWriterMethod] = Field(
        [], description="DataFrame backend reader methods."
    )
    type: Literal["PIPELINE_NODE"] = Field(
        "PIPELINE_NODE", frozen=True, description="Source Type"
    )

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
    # Readers                                                                 #z
    # ----------------------------------------------------------------------- #

    def _read_spark(self) -> AnyFrame:
        stream_to_batch = not self.as_stream and self.node.has_streaming_source

        # Inside the Lakeflow (LDP) runtime, always read the upstream from its
        # registered dataset so the query plan carries a traceable
        # spark.read.table(<dataset>). Returning the cached in-memory output_df
        # would hide the dependency from Lakeflow static DAG inference, leaving
        # batch->batch node chains disconnected in the pipeline graph.
        #
        # This is intentionally NOT applied to SDP: the local `spark-pipelines`
        # runtime eagerly analyzes each dataset's plan (Laktory's SQL transformer
        # calls spark.sql), and a spark.read.table() against a not-yet-materialized
        # pipeline dataset fails analysis at graph-registration time. SDP instead
        # relies on the upstream plan being inlined from the original source. The
        # `is_ldp_execute` flag fires only inside the actual Lakeflow runtime, so
        # local Pipeline.execute() behavior is preserved.
        if self.node.is_ldp_execute and self.node.primary_sink:
            return self._read_spark_declarative_dataset()

        # When a streaming read is requested, always go to the primary sink so
        # that spark.readStream.table() is called. Returning a cached batch
        # output_df here would produce an invalid batch-relation-for-streaming-table
        # error in SDP (and incorrect behavior in any streaming context).
        if stream_to_batch or self.node.output_df is None or self.as_stream:
            if self.node.has_sinks:
                logger.info(f"Reading pipeline node {self._id} from primary sink")
                df = self.node.primary_sink.read(
                    as_stream=self.as_stream,
                    reader_kwargs=self.reader_kwargs,
                    reader_methods=self.reader_methods,
                )
            else:
                logger.info("Executing parent pipeline node")
                self.node.execute()
                df = self.node.output_df

        else:
            logger.info(f"Reading pipeline node {self._id} from output DataFrame")
            df = self.node.output_df

        return df

    def _read_spark_declarative_dataset(self) -> AnyFrame:
        """
        Read the upstream node from the dataset registered by the Lakeflow
        (LDP) declarative-pipeline decorators.

        The read target must match the name declared to the dp decorators
        (`sdp_table_or_view_name`): the sink's fully-qualified `full_name` when a
        catalog is used, but the *unqualified* table name otherwise (Hive
        metastore). A fully-qualified name would not resolve against the
        pipeline's virtual schema during graph registration.
        """
        sink = self.node.primary_sink
        source = sink.as_source(
            as_stream=self.as_stream,
            reader_kwargs=self.reader_kwargs,
            reader_methods=self.reader_methods,
        )
        # Without a catalog the dataset is registered unqualified; drop the
        # schema so the read targets the bare dataset name (e.g. `slv`).
        if source.catalog_name is None:
            source._setattr("schema_name", None)
        logger.info(
            f"Reading pipeline node {self._id} from declarative dataset {source.full_name}"
        )
        return source._read_spark()

    def _read_polars(self) -> AnyFrame:
        # No declarative-runtime handling here: LDP and SDP are Spark-only
        # orchestrators, so a Polars read never happens inside their runtime
        # (the declarative dependency-inference concern handled in `_read_spark`
        # does not apply).

        # Read from node output DataFrame (if available)
        if self.node.output_df is not None:
            logger.info(f"Reading pipeline node {self._id} from output DataFrame")
            df = self.node.output_df

        # Read from node sink
        elif self.node.primary_sink:
            logger.info(f"Reading pipeline node {self._id} from sink")
            df = self.node.primary_sink.read(
                as_stream=self.as_stream,
                reader_kwargs=self.reader_kwargs,
                reader_methods=self.reader_methods,
            )

        # Execute upstream node
        else:
            logger.info("Executing parent pipeline node")
            self.node.execute()
            df = self.node.output_df

        return df
