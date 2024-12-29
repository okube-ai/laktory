# COMMAND ----------
# Install dependencies
laktory_root = "/Workspace" + spark.conf.get("workspace_laktory_root", "/.laktory/")
pl_name = spark.conf.get("pipeline_name", "dlt-stock-prices")
filepath = f"{laktory_root}/pipelines/{pl_name}/requirements.txt"
# MAGIC %pip install -r $filepath
# MAGIC %restart_python

# COMMAND ----------
import importlib
import sys
import os
import pyspark.sql.functions as F

from laktory import dlt
from laktory import models
from laktory import get_logger

dlt.spark = spark
logger = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Create Widgets                                                              #
# --------------------------------------------------------------------------- #

dbutils.widgets.text("pipeline_name", "dlt-stock-prices")
dbutils.widgets.text("node_name", "")
dbutils.widgets.text("workspace_laktory_root", "/.laktory/")

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

laktory_root = "/Workspace" + spark.conf.get(
    "workspace_laktory_root", dbutils.widgets.get("workspace_laktory_root")
)
pl_name = spark.conf.get("pipeline_name", dbutils.widgets.get("pipeline_name"))
node_name = dbutils.widgets.get("node_name")
filepath = f"{laktory_root}/pipelines/{pl_name}/config.json"
with open(filepath, "r") as fp:
    pl = models.Pipeline.model_validate_json(fp.read())

# Import User Defined Functions
sys.path.append(f"{laktory_root}/pipelines/")
udfs = []
for udf in pl.udfs:
    if udf.module_path:
        sys.path.append(os.path.abspath(udf.module_path))
    module = importlib.import_module(udf.module_name)
    module = importlib.reload(module)
    globals()[udf.module_name] = module
    udfs += [getattr(module, udf.function_name)]

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

    @dlt.table_or_view(
        name=name,
        comment=node.description,
        as_view=sink is None,
    )
    @dlt.expect_all(dlt_warning_expectations)
    @dlt.expect_all_or_drop(dlt_drop_expectations)
    @dlt.expect_all_or_fail(dlt_fail_expectations)
    def get_df():

        sink_str = ""
        if sink is not None:
            sink_str = f" | sink: {sink.full_name}"
        logger.info(f"Building {node.name} node{sink_str}")

        # Execute node
        node.execute(spark=spark, udfs=udfs)
        if sink and sink.is_quarantine:
            df = node.quarantine_df
        else:
            df = node.output_df
        df.printSchema()

        # Return
        return df

    return get_df


# --------------------------------------------------------------------------- #
# CDC tables                                                                  #
# --------------------------------------------------------------------------- #


def define_cdc_table(node, sink):
    dlt.create_streaming_table(
        name=sink.table_name,
        comment=node.description,
    )

    df = dlt.apply_changes(
        source=node.source.table_name, **sink.dlt_apply_changes_kwargs
    )

    return df


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Build nodes
for node in pl.nodes:

    if node_name and node.name != node_name:
        continue

    if node.dlt_template != "DEFAULT":
        continue

    if node.sinks is None or node.sinks == []:
        wrapper = define_table(node, None)
        df = dlt.get_df(wrapper)
        display(df)

    else:

        for sink in node.sinks:

            if sink.is_cdc:
                df = define_cdc_table(node, sink)
                display(df)

            else:
                wrapper = define_table(node, sink)
                df = dlt.get_df(wrapper)
                display(df)
