
import json
import pyspark.sql.functions as F

from laktory import dlt
from laktory import models
from laktory import readers
from laktory import settings
# from laktory.readers import EventsReader
# from laktory.readers import EventsStreamReader
# from laktory._testing import StockPriceDefinition
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

        print(table)

        reader = readers.EventsReader(
            event_name=table.event_source.name,
            producer_name=table.event_source.producer.name,
        )
        df = reader.read(spark)

        display(df)

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
