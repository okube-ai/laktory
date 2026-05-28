# Databricks notebook source
# COMMAND ----------
import json

reqs = spark.conf.get("laktory.requirements")
reqs = " ".join(json.loads(reqs))
# MAGIC %pip install $reqs
# MAGIC %restart_python

# COMMAND ----------
import pyspark.sql.functions as F  # noqa: F401, E402
from pyspark import pipelines as dp  # noqa: E402

import laktory as lk  # noqa: E402

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

config_filepath = spark.conf.get("laktory.config_filepath")
print(f"Reading pipeline at {config_filepath}")
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

    if not sink.is_cdc:

        @table_or_view(**sink.sdp_table_or_view_kwargs)
        @dp.expect_all(sink.ldp_warning_expectations)
        @dp.expect_all_or_drop(sink.ldp_drop_expectations)
        @dp.expect_all_or_fail(sink.ldp_fail_expectations)
        def get_df():
            node.execute()
            if sink.is_quarantine:
                df = node.quarantine_df
            else:
                df = node.output_df
            return df.to_native()

    else:

        @dp.temporary_view(name=sink.sdp_pre_merge_view_name)
        def get_df():
            node.execute()
            return node.output_df.to_native()

        dp.create_streaming_table(**sink.sdp_table_or_view_kwargs)
        dp.create_auto_cdc_flow(**sink.ldp_auto_cdc_flow_kwargs)


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Build nodes
for node in pl.nodes:
    if node.ldp_template != "DEFAULT":
        continue

    for sink in node.sinks:
        define_table(node, sink)
