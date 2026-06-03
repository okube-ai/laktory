import pathlib

import pyspark.sql.functions as F  # noqa: F401
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

import laktory as lk

spark = SparkSession.getActiveSession()

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

config_filepath = spark.conf.get("laktory.config_filepath")
print(f"Reading pipeline at {config_filepath}")

# Write is_sdp_execute() result alongside the config for observability and testing.
pathlib.Path(config_filepath).parent.joinpath(".laktory_is_sdp_execute").write_text(
    "true" if lk.is_sdp_execute() else "false"
)
with open(config_filepath, "r") as fp:
    pl = lk.models.Pipeline.model_validate_json(fp.read())

# --------------------------------------------------------------------------- #
# Tables and Views Definition                                                 #
# --------------------------------------------------------------------------- #


def define_table(node, sink):
    table_or_view = dp.materialized_view
    if isinstance(sink, lk.models.PipelineViewDataSink):
        table_or_view = dp.temporary_view
    elif sink.is_streaming():
        table_or_view = dp.table

    # Expectations are not supported by SDP. If/when they are
    # this decorator will be updated to take them into account.
    @table_or_view(**sink.sdp_table_or_view_kwargs)
    def get_df():
        node.execute()
        return node.output_df.to_native()


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

for node in pl.nodes:
    for sink in node.sinks:
        define_table(node, sink)
