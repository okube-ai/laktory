from datetime import datetime
from datetime import timedelta
import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta.tables import DeltaTable
from laktory import models


# --------------------------------------------------------------------------- #
# Spark                                                                       #
# --------------------------------------------------------------------------- #

spark = (
    SparkSession.builder.appName("WriteMerge")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
spark.conf.set("spark.sql.session.timeZone", "UTC")


# --------------------------------------------------------------------------- #
# Functions                                                                   #
# --------------------------------------------------------------------------- #

path = "./stocks_target"
pkeys = ["symbol", "timestamp"]
price_cols = ["close", "open", "high", "low"]

def build_df(nstamps=10, nstocks=10):

    # Timestamps
    t0 = datetime(2024, 11, 1)
    t1 = t0 + timedelta(days=nstamps-1)
    timestamps = pd.date_range(t0, t1, freq="d")

    # Symbols
    symbols = [f"S{i:03d}" for i in range(nstocks)]

    # Create an empty DataFrame with the schema
    df_symbols = spark.createDataFrame(pd.DataFrame({"symbol": symbols}))
    df_timestamps = spark.createDataFrame(pd.DataFrame({"timestamp": timestamps}))
    df = df_timestamps.crossJoin(df_symbols)

    # Add columns with random numbers
    for c in price_cols:
        df = df.withColumn(c, F.round(F.rand(), 2))

    df = df.withColumn("_from", F.lit("target"))

    first_symbols = F.col("symbol").isin([f"S{i:03d}" for i in [0, 1, 2]])
    last_symbols = F.col("symbol").isin([f"S{i:03d}" for i in [nstocks-1]])

    is_new = F.col("timestamp") >= timestamps[-2]
    df = df.withColumn("_is_new", is_new)
    df = df.withColumn("_is_updated", first_symbols & ~is_new)
    df = df.withColumn("_is_deleted", last_symbols & ~is_new)

    return df


def write_target(df):
    (
        df
        .filter(~F.col("_is_new"))
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def get_source(df):
    c1 = F.col("_is_new")
    c2 = F.col("_is_updated")
    c3 = F.col("_is_deleted")

    df = df.filter(c1 | c2 | c3)

    for c in price_cols:
        df = df.withColumn(c, F.when(F.col("_is_updated"), 10).otherwise(F.col(c)))
    for c in price_cols:
        df = df.withColumn(c, F.when(F.col("_is_new"), 100).otherwise(F.col(c)))

    return df


def read():
    return (
        spark
        .read
        .format("DELTA")
        .load(path)
    )


def merge(source):

    table_target = DeltaTable.forPath(spark, path)
    (
        table_target.alias("target")
        .merge(
            source.alias("source"),
            # how="outer",
            condition=" AND ".join([f"source.{c} = target.{c}" for c in pkeys]),
        )
        .whenMatchedUpdate(
            set={f"target.{c}": f"source.{c}" for c in price_cols + ["_from"]},
            condition="source._is_updated = true",
        )
        .whenNotMatchedInsert(
            # values = {f"target.{c}": f"source.{c}" for c in df.columns if not c.startswith("_")}
            values={f"target.{c}": f"source.{c}" for c in source.columns}
        )
        .whenMatchedDelete(
            condition="source._is_deleted = true",
        )
    ).execute()


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

df = build_df()
write_target(df)

df0 = read()
print()
print("Initial data")
print("Rows count: ", df0.count())
print("New rows: ", df0.filter(F.col("_is_new")).count())
print("To delete rows: ", df0.filter(F.col("_is_deleted")).count())
print("Tp update rows: ", df0.filter(F.col("_is_updated")).count())
assert df0.count() == 80
assert df0.filter(F.col("_is_new")).count() == 0
assert df0.filter(F.col("_is_deleted")).count() == 8
assert df0.filter(F.col("_is_updated")).count() == 24

print("-----")

dfs = get_source(df)
print()
print("Source data")
print("Rows count: ", dfs.count())
print("New rows: ", dfs.filter(F.col("_is_new")).count())
print("Delete rows: ", dfs.filter(F.col("_is_deleted")).count())
print("Update rows: ", dfs.filter(F.col("_is_updated")).count())
assert dfs.count() == 52
assert dfs.filter(F.col("_is_new")).count() == 20
assert dfs.filter(F.col("_is_deleted")).count() == 8
assert dfs.filter(F.col("_is_updated")).count() == 24
print("-----")

merge(dfs)


df1 = read()
print()
print("Final data")
print("Rows count: ", df1.count())
print("To delete rows: ", df1.filter(F.col("_is_deleted")).count())
print("Updated rows: ", df1.filter(F.col("_is_updated")).count())
assert df1.count() == 92
assert df1.filter("_is_new").count() == 20
assert df1.filter("_is_new").filter(F.col("close") == 100).count() == 20
assert df1.filter("_is_deleted").count() == 0
assert df1.filter("_is_updated").count() == 24
assert df1.filter("_is_updated").filter(F.col("close") == 10).count() == 24
print("-----")

