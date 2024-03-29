# MAGIC %pip install laktory

# COMMAND ----------
import pyspark.sql.functions as F

from laktory import dlt
from laktory import read_metadata
from laktory import get_logger

dlt.spark = spark
logger = get_logger(__name__)


# Read pipeline definition
pl_name = spark.conf.get("pipeline_name", "pl-stock-prices")
pl = read_metadata(pipeline=pl_name)


# --------------------------------------------------------------------------- #
# Non-CDC Tables                                                              #
# --------------------------------------------------------------------------- #


def define_table(table):
    @dlt.table_or_view(
        name=table.name,
        comment=table.comment,
        as_view=table.builder.as_dlt_view,
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
        df = table.builder.process(df, udfs=None, spark=spark)

        # Return
        return df

    return get_df


# --------------------------------------------------------------------------- #
# CDC tables                                                                  #
# --------------------------------------------------------------------------- #


def define_cdc_table(table):
    dlt.create_streaming_table(
        name=table.name,
        comment=table.comment,
    )

    df = dlt.apply_changes(**table.builder.apply_changes_kwargs)

    return df


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Build tables
for table in pl.tables:
    if table.builder.template == "BRONZE":
        if table.is_from_cdc:
            df = define_cdc_table(table)
            display(df)

        else:
            wrapper = define_table(table)
            df = dlt.get_df(wrapper)
            display(df)
