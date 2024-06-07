import os
import pandas as pd
from pyspark.sql import SparkSession
import polars as pl

spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


# --------------------------------------------------------------------------- #
# DataFrames                                                                  #
# --------------------------------------------------------------------------- #

data_dirpath = os.path.join(os.path.dirname(__file__), "../../tests/data/")

# Spark
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

# Polars
dirpath = os.path.join(data_dirpath, "slv_stock_prices")
for filename in os.listdir(dirpath):
    if filename.endswith(".parquet"):
        break
df_slv_polars = pl.read_parquet(os.path.join(dirpath, filename))

dirpath = os.path.join(data_dirpath, "slv_stock_meta")
for filename in os.listdir(dirpath):
    if filename.endswith(".parquet"):
        break
df_meta_polars = pl.read_parquet(os.path.join(dirpath, filename))
