# MAGIC %pip install laktory

# COMMAND ----------
import pyspark.sql.functions as F
import importlib
import sys
import os

from laktory import dlt
from laktory import read_metadata
from laktory import get_logger

dlt.spark = spark
logger = get_logger(__name__)

# Read pipeline definition
pl_name = spark.conf.get("pipeline_name", "pl-stock-prices")
pl = read_metadata(pipeline=pl_name)

# Import User Defined Functions
sys.path.append("/Workspace/pipelines/")
udfs = []
for udf in pl.udfs:
    if udf.module_path:
        sys.path.append(os.path.abspath(udf.module_path))
    module = importlib.import_module(udf.module_name)
    udfs += [getattr(module, udf.function_name)]


# --------------------------------------------------------------------------- #
# Non-CDC Tables                                                              #
# --------------------------------------------------------------------------- #


def define_table(table):
    @dlt.table(
        name=table.name,
        comment=table.comment,
    )
    @dlt.expect_all(table.warning_expectations)
    @dlt.expect_all_or_drop(table.drop_expectations)
    @dlt.expect_all_or_fail(table.fail_expectations)
    def get_df():
        logger.info(f"Building {table.name} table")

        # Read Source
        df = table.builder.read_source(spark)
        df.printSchema()

        # Process
        df = table.builder.process(df, udfs=udfs, spark=spark)

        # Return
        return df

    return get_df


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Build tables
for table in pl.tables:
    if table.builder.template == "SILVER_STAR":
        wrapper = define_table(table)
        df = dlt.get_df(wrapper)
        display(df)
