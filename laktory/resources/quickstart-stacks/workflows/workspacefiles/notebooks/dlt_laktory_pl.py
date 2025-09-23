# Databricks notebook source
# COMMAND ----------
import json

reqs = spark.conf.get("requirements")
reqs = " ".join(json.loads(reqs))
# MAGIC %pip install $reqs
# MAGIC %restart_python

# COMMAND ----------
import dlt  # noqa: E402
import pyspark.sql.functions as F  # noqa: F401, E402

import laktory as lk  # noqa: E402

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
    table_or_view = dlt.table
    if isinstance(sink, lk.models.PipelineViewDataSink):
        table_or_view = dlt.view

    if not sink.is_cdc:

        @table_or_view(**sink.dlt_table_or_view_kwargs)
        @dlt.expect_all(sink.dlt_warning_expectations)
        @dlt.expect_all_or_drop(sink.dlt_drop_expectations)
        @dlt.expect_all_or_fail(sink.dlt_fail_expectations)
        def get_df():
            # Execute node
            node.execute()
            if sink.is_quarantine:
                df = node.quarantine_df
            else:
                df = node.output_df

            # Return
            return df.to_native()

    else:

        @dlt.view(name=sink.dlt_pre_merge_view_name)
        def get_df():
            node.execute()
            return node.output_df.to_native()

        dlt.create_streaming_table(**sink.dlt_table_or_view_kwargs)
        dlt.apply_changes(**sink.dlt_apply_changes_kwargs)


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Build nodes
for node in pl.nodes:
    if node.dlt_template != "DEFAULT":
        continue

    for sink in node.sinks:
        define_table(node, sink)
