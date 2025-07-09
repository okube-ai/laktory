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
    # Get Expectations
    dlt_warning_expectations = {}
    dlt_drop_expectations = {}
    dlt_fail_expectations = {}
    if sink and not sink.is_quarantine:
        dlt_warning_expectations = node.dlt_warning_expectations
        dlt_drop_expectations = node.dlt_drop_expectations
        dlt_fail_expectations = node.dlt_fail_expectations

    table_or_view = dlt.table
    if isinstance(sink, lk.models.PipelineViewDataSink):
        table_or_view = dlt.view

    @table_or_view(
        name=sink.dlt_name,
        comment=node.comment,
    )
    @dlt.expect_all(dlt_warning_expectations)
    @dlt.expect_all_or_drop(dlt_drop_expectations)
    @dlt.expect_all_or_fail(dlt_fail_expectations)
    def get_df():
        # Execute node
        node.execute()
        if sink and sink.is_quarantine:
            df = node.quarantine_df
        else:
            df = node.output_df

        # Return
        return df.to_native()


# --------------------------------------------------------------------------- #
# CDC tables                                                                  #
# --------------------------------------------------------------------------- #


def define_cdc_table(node, sink):
    dlt.create_streaming_table(
        name=sink.dlt_name,
        comment=node.comment,
    )

    dlt.apply_changes(
        source=node.source.node.primary_sink.dlt_name, **sink.dlt_apply_changes_kwargs
    )


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Build nodes
for node in pl.nodes:
    if node.dlt_template != "DEFAULT":
        continue

    for sink in node.sinks:
        if sink.is_cdc:
            define_cdc_table(node, sink)
        else:
            define_table(node, sink)
