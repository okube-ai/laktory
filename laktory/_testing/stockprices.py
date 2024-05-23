import os
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


# --------------------------------------------------------------------------- #
# DataFrames                                                                  #
# --------------------------------------------------------------------------- #

data_dirpath = os.path.join(os.path.dirname(__file__), "../../tests/data/")
df_brz = spark.read.parquet(os.path.join(data_dirpath, "brz_stock_prices"))
df_slv = spark.read.parquet(os.path.join(data_dirpath, "slv_stock_prices"))
df_meta = spark.read.parquet(os.path.join(data_dirpath, "slv_stock_meta"))
df_name = spark.createDataFrame(
    pd.DataFrame(
        {
            "symbol3": ["AAPL", "GOOGL", "AMZN"],
            "name": ["Apple", "Google", "Amazon"],
        }
    )
)
