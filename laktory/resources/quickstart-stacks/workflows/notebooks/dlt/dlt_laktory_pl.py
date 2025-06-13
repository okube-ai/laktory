# COMMAND ----------
import json

reqs = spark.conf.get("requirements")
reqs = " ".join(json.loads(reqs))
# MAGIC %pip install $reqs
# MAGIC %restart_python

# COMMAND ----------
import dlt
import pyspark.sql.functions as F  # noqa: F401

import laktory as lk

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

config = spark.conf.get("config")
pl = lk.models.Pipeline.model_validate_json(config)

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

    # Get Name
    name = node.name
    if sink is not None:
        name = sink.table_name
        if node.parent_pipeline.orchestrator.target != sink.schema_name:
            name = sink.full_name

    table_or_view = dlt.table
    if sink is None:
        table_or_view = dlt.view

    @table_or_view(
        name=name,
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

    return get_df


# --------------------------------------------------------------------------- #
# CDC tables                                                                  #
# --------------------------------------------------------------------------- #


def define_cdc_table(node, sink):
    dlt.create_streaming_table(
        name=sink.table_name,
        comment=node.comment,
    )

    df = dlt.apply_changes(
        source=node.source.table_name, **sink.dlt_apply_changes_kwargs
    )

    return df.to_native()


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Build nodes
for node in pl.nodes:
    if node.dlt_template != "DEFAULT":
        continue

    if node.sinks is None or node.sinks == []:
        define_table(node, None)

    else:
        for sink in node.sinks:
            if sink.is_cdc:
                define_cdc_table(node, sink)

            else:
                define_table(node, sink)
