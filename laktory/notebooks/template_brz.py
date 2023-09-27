import json
import pyspark.sql.functions as F

from laktory import dlt
from laktory import models
from laktory._logger import get_logger

dlt.spark = spark
logger = get_logger(__name__)

pl_name = spark.conf.get("pipeline_name", "pl-stock-prices")


def define_bronze_table(table):
    @dlt.table(
        name=table.name,
        comment=table.comment,
    )
    def get_df():
        logger.info(f"Building {table.name} table")

        # Read Source
        df = table.read_source(spark)

        # Process
        df = table.process_bronze(df)

        return df

    return get_df


tables = (
    spark.read.table("main.laktory.tables")
    .filter(F.col("pipeline_name") == pl_name)
    .filter(F.col("zone") == "BRONZE")
)

for row in tables.collect():
    d = row.asDict(recursive=True)
    d["columns"] = json.loads(d["columns"])
    if d["table_source"] is None:
        del d["table_source"]
    if d["event_source"] is None:
        del d["event_source"]
    table = models.Table(**d)
    get_df = define_bronze_table(table)

    if dlt.is_debug():
        df = get_df()
        display(df)
