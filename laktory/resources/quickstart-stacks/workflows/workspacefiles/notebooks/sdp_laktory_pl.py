import pyspark.sql.functions as F  # noqa: F401
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

import laktory as lk

spark = SparkSession.getActiveSession()

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

config_filepath = spark.conf.get("config_filepath")
print(f"Reading pipeline at {config_filepath}")
with open(config_filepath, "r") as fp:
    pl = lk.models.Pipeline.model_validate_json(fp.read())

# --------------------------------------------------------------------------- #
# Tables and Views Definition                                                 #
# --------------------------------------------------------------------------- #


def define_table(node, sink):
    if isinstance(sink, lk.models.PipelineViewDataSink):
        table_or_view = dp.temporary_view
    elif node.source and getattr(node.source, "as_stream", False):
        table_or_view = dp.table
    else:
        table_or_view = dp.materialized_view

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
