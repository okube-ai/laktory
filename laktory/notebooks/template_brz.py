
import json
import pyspark.sql.functions as F

from laktory import dlt
from laktory import models
from laktory import settings
dlt.spark = spark

pl_name = "pl-stock-prices"


# TODO: Add supports for DBR 12.2? Or Not?
def define_bronze_table(table):

    @dlt.table(
        name=table.name,
        comment=table.comment,
        #path=TODO,
    )
    def get_df():

        # Read Source
        source = table.source
        df = source.read(spark)

        return df


tables = (
    spark.read.table("main.laktory.tables")
    .filter(F.col("pipeline_name") == pl_name)
    .filter(F.col("zone") == "BRONZE")
)

for row in tables.collect():
    d = row.asDict(recursive=True)
    d["columns"] = json.loads(d["columns"])
    df = define_bronze_table(models.Table(**d))
