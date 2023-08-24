import json
import pyspark.sql.functions as F

from laktory import dlt
from laktory import models
from laktory import settings
from laktory._logger import get_logger

dlt.spark = spark
logger = get_logger(__name__)

pl_name = "pl-stock-prices"


# TODO: Add supports for DBR 12.2? Or Not?
def define_bronze_table(table):
    @dlt.table(
        name=table.name,
        comment=table.comment,
        # path=TODO,
    )
    def get_df():
        logger.info(f"Building {table.name} table")

        # Read Source
        source = table.source
        df = source.read(spark)

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
    table = models.Table(**d)
    dataset = define_bronze_table(table)()

    if not dlt.is_pipeline():
        df = dataset.func()
        display(df)
