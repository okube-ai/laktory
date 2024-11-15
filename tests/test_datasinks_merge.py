import datetime
import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta.tables import DeltaTable
from laktory import models

from laktory.types import AnyDataFrame
from laktory.types import SparkDataFrame


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
price_cols = ["close", "open"]


def build_df():

    nstocks = 3
    nstamps = 5

    # Timestamps
    t0 = datetime.date(2024, 11, 1)
    # t1 = t0 + timedelta(days=nstamps-1)
    # timestamps = pd.date_range(t0, t1, freq="d")
    dates = [t0 + datetime.timedelta(days=i) for i in range(nstamps)]

    # Symbols
    symbols = [f"S{i:1d}" for i in range(nstocks)]

    # Create an empty DataFrame with the schema
    df_symbols = spark.createDataFrame(pd.DataFrame({"symbol": symbols}))
    df_dates = spark.createDataFrame(pd.DataFrame({"date": dates}))
    df = df_dates.crossJoin(df_symbols)

    # Add columns with random numbers
    for i, c in enumerate(price_cols):
        df = df.withColumn(c, F.round(F.rand(seed=i), 2))

    df = df.withColumn("from", F.lit("target"))

    first_symbols = F.col("symbol").isin([f"S{i:1d}" for i in [0]])
    last_symbols = F.col("symbol").isin([f"S{i:1d}" for i in [nstocks - 1]])

    # Last 2 timestamps are new rows
    # First symbol will be updated
    # Last symbol will be deleted
    is_new = F.col("date") >= dates[-2]
    df = df.withColumn("_is_new", is_new)
    df = df.withColumn("_is_updated", first_symbols & ~is_new)
    df = df.withColumn("_is_deleted", last_symbols & ~is_new)

    df = df.sort("date", "symbol")

    return df


def write_target(df):
    (
        df.filter(~F.col("_is_new"))
        .drop("_is_new", "_is_updated", "_is_deleted")
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def get_source(df):
    c1 = F.col("_is_new")
    c2 = F.col("_is_updated")
    c3 = F.col("_is_deleted")

    df = df.filter(c1 | c2 | c3)
    df = df.drop("_is_new", "_is_updated")
    df = df.withColumn("from", F.lit("source"))

    return df


def read():
    return spark.read.format("DELTA").load(path)


def execute_merge(
    target_path,
    source: SparkDataFrame,
    primary_keys,
    include_columns=None,
    exclude_columns=None,
    delete_where=None,
):

    # Select columns
    if include_columns:
        columns = [c for c in include_columns]
    else:
        columns = [c for c in source.columns if c not in primary_keys]
        if exclude_columns:
            columns = [c for c in columns if c not in exclude_columns]

    table_target = DeltaTable.forPath(spark, target_path)

    # Define merge
    merge = (
        table_target.alias("target")
        .merge(
            source.alias("source"),
            condition=" AND ".join([f"source.{c} = target.{c}" for c in primary_keys]),
        )
    )

    # Update
    if not delete_where:
        merge = merge.whenMatchedUpdate(
            set={f"target.{c}": f"source.{c}" for c in columns},
        )
    else:
        merge = merge.whenMatchedUpdate(
            set={f"target.{c}": f"source.{c}" for c in columns},
            condition=~F.expr(delete_where)
        )

    merge = merge.whenNotMatchedInsert(
        values={f"target.{c}": f"source.{c}" for c in primary_keys + columns}
    )

    if delete_where:
        merge = merge.whenMatchedDelete(
            condition=delete_where,
        )

    merge.execute()


def test_merge():

    # Build and write target
    df = build_df()
    write_target(df)

    df0 = read().toPandas()
    assert len(df0) == 9  # 3 stocks * 3 timestamps
    assert df0["from"].unique().tolist() == ["target"]

    # Build Source
    dfs = get_source(df)
    _dfs = dfs.toPandas()
    assert len(_dfs) == 12  # 6 new + 3 updates + 3 deletes
    assert _dfs["from"].unique().tolist() == ["source"]

    # Merge source with target
    execute_merge(
        target_path=path,
        source=dfs,
        primary_keys=["symbol", "date"],
        delete_where="source._is_deleted = true",
        exclude_columns=["_is_deleted"],
    )

    # Read updated target
    df1 = read().sort("date", "symbol").toPandas()
    assert df1.columns.tolist() == ['date', 'symbol', 'close', 'open', 'from']
    assert len(df1) == 9 + 6 - 3  # 9 initial + 6 new - 3 deletes
    assert "S3" not in df1["symbol"].unique().tolist()  # deleted symbol
    assert (df1["from"] == "source").sum() == 9  # 6 new + 3 updates


if __name__ == "__main__":
    test_merge()
