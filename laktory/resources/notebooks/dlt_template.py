# MAGIC %pip install laktory

# COMMAND ----------
import pyspark.sql.functions as F

from laktory import dlt
from laktory import models
from laktory import get_logger
from laktory import settings

dlt.spark = spark
logger = get_logger(__name__)

# Read pipeline definition
pl_name = spark.conf.get("pipeline_name", "pl-stock-prices")
filepath = f"/Workspace{settings.workspace_laktory_root}pipelines/{pl_name}.json"
with open(filepath, "r") as fp:
    pl = models.Pipeline.model_validate_json(fp.read())


# --------------------------------------------------------------------------- #
# Tables and Views Definition                                                 #
# --------------------------------------------------------------------------- #


def define_table(node):
    @dlt.table_or_view(
        name=node.id,
        comment=node.description,
        as_view=node.sink is None,
    )
    @dlt.expect_all(node.warning_expectations)
    @dlt.expect_all_or_drop(node.drop_expectations)
    @dlt.expect_all_or_fail(node.fail_expectations)
    def get_df():
        logger.info(f"Building {node.id} node")

        # Execute node
        df = node.execute(spark=spark, udfs=None)
        df.printSchema()

        # Return
        return df

    return get_df


# --------------------------------------------------------------------------- #
# CDC tables                                                                  #
# --------------------------------------------------------------------------- #


# def define_cdc_table(table):
#     dlt.create_streaming_table(
#         name=table.name,
#         comment=table.comment,
#     )
#
#     df = dlt.apply_changes(**table.builder.apply_changes_kwargs)
#
#     return df


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Build nodes
for node in pl.nodes:
    if node.is_from_cdc:
        pass
        # df = define_cdc_table(table)
        # display(df)

    else:
        wrapper = define_table(node)
        df = dlt.get_df(wrapper)
        display(df)
