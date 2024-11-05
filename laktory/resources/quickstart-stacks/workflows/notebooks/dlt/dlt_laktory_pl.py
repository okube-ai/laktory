# MAGIC %pip install 'laktory==<laktory_version>'

# COMMAND ----------
import importlib
import sys
import os
import pyspark.sql.functions as F

from laktory import dlt
from laktory import models
from laktory import get_logger
from laktory import settings

dlt.spark = spark
logger = get_logger(__name__)

# Read pipeline definition
pl_name = spark.conf.get("pipeline_name", "pl-stocks-dlt")
filepath = f"/Workspace{settings.workspace_laktory_root}pipelines/{pl_name}.json"
with open(filepath, "r") as fp:
    pl = models.Pipeline.model_validate_json(fp.read())


# Import User Defined Functions
sys.path.append(f"/Workspace{settings.workspace_laktory_root}pipelines/")
udfs = []
for udf in pl.udfs:
    if udf.module_path:
        sys.path.append(os.path.abspath(udf.module_path))
    module = importlib.import_module(udf.module_name)
    module = importlib.reload(module)
    udfs += [getattr(module, udf.function_name)]

# --------------------------------------------------------------------------- #
# Tables and Views Definition                                                 #
# --------------------------------------------------------------------------- #


def define_table(node, sink):

    # Get Expectations
    dlt_warning_expectations = {}
    dlt_drop_expectations = {}
    dlt_fail_expectations = {}
    if not sink.is_quarantine:
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
        if sink.is_quarantine:
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


def define_cdc_table(node):
    dlt.create_streaming_table(
        name=node.name,
        comment=node.comment,
    )

    df = dlt.apply_changes(**node.apply_changes_kwargs)

    return df


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Build nodes
for node in pl.nodes:

    if node.dlt_template != "DEFAULT":
        continue

    if node.is_from_cdc:
        df = define_cdc_table(node)
        display(df)
        continue

    if node.sinks is None:
        wrapper = define_table(node, None)
        df = dlt.get_df(wrapper)
        display(df)

    else:
        for sink in node.sinks:
            wrapper = define_table(node, sink)
            df = dlt.get_df(wrapper)
            display(df)
