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
with open(config_filepath, "r", encoding="utf-8-sig") as fp:
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
            return sink.with_writer_column(df).to_native()

    else:

        @dp.temporary_view(name=sink.sdp_pre_merge_view_name)
        def get_df():
            node.execute()
            return node.output_df.to_native()

        dp.create_streaming_table(**sink.sdp_table_or_view_kwargs)
        dp.create_auto_cdc_flow(**sink.ldp_auto_cdc_flow_kwargs)


def define_append_flow(node, sink):
    # Expectations are defined on the shared streaming table, as append flows don't
    # support them.
    @dp.append_flow(target=sink.sdp_table_or_view_name, name=sink.sdp_append_flow_name)
    def get_df():
        node.execute()
        if sink.is_quarantine:
            df = node.quarantine_df
        else:
            df = node.output_df
        return sink.with_writer_column(df).to_native()


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Tables shared by multiple nodes are declared once and written by append flows
shared_tables = pl.sdp_append_flow_sinks
for table_name in shared_tables:
    dp.create_streaming_table(**pl.get_sdp_streaming_table_kwargs(table_name))

# Build nodes
for node in pl.nodes:
    if node.ldp_template != "DEFAULT":
        continue

    for sink in node.sinks:
        if getattr(sink, "sdp_table_or_view_name", None) in shared_tables:
            define_append_flow(node, sink)
        else:
            define_table(node, sink)
