import datetime
import uuid
import os
import shutil
import pandas as pd
from pathlib import Path

import pyspark.sql.functions as F
import pyspark.sql.types as T
from laktory import models

from laktory._testing import spark
from laktory._testing import Paths


# --------------------------------------------------------------------------- #
# Functions                                                                   #
# --------------------------------------------------------------------------- #


paths = Paths(__file__)

testdir_path = Path(__file__).parent

price_cols = ["close", "open"]


def build_target(path=None):

    if path is None:
        path = testdir_path / "tmp" / "test_datasinks_merge" / str(uuid.uuid4())

    nstocks = 3
    nstamps = 5

    # Timestamps
    t0 = datetime.date(2024, 11, 1)
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
    df = df.sort("date", "symbol")

    first_symbols = F.col("symbol").isin([f"S{i:1d}" for i in [0]])
    last_symbols = F.col("symbol").isin([f"S{i:1d}" for i in [nstocks - 1]])

    # Last 2 timestamps are new rows
    # First symbol will be updated
    # Last symbol will be deleted
    is_new = F.col("date") >= dates[-2]
    df = df.withColumn("_is_new", is_new)
    df = df.withColumn("_is_updated", first_symbols & ~is_new)
    df = df.withColumn("_is_deleted", last_symbols & ~is_new)

    # Write Target
    (
        df.filter(~F.col("_is_new"))
        .drop("_is_new", "_is_updated", "_is_deleted")
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )

    # Build Source
    c1 = F.col("_is_new")
    c2 = F.col("_is_updated")
    c3 = F.col("_is_deleted")

    dfs = df.filter(c1 | c2 | c3)
    dfs = dfs.drop("_is_new", "_is_updated")
    dfs = dfs.withColumn("from", F.lit("source"))

    return path, dfs


def read(path):
    return spark.read.format("DELTA").load(str(path))


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #

def test_basic():

    path, dfs = build_target()

    # Test target
    df0 = read(path).toPandas()
    assert len(df0) == 9  # 3 stocks * 3 timestamps
    assert df0["from"].unique().tolist() == ["target"]

    # Test Source
    _dfs = dfs.toPandas()
    assert len(_dfs) == 12  # 6 new + 3 updates + 3 deletes
    assert _dfs["from"].unique().tolist() == ["source"]

    sink = models.FileDataSink(
        mode="MERGE",
        path=str(path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            delete_where="source._is_deleted = true",
            exclude_columns=["_is_deleted"],
        ),
    )
    sink.write(dfs)

    # Read updated target
    df1 = read(path).sort("date", "symbol").toPandas()
    assert df1.columns.tolist() == ['date', 'symbol', 'close', 'open', 'from']
    assert len(df1) == 9 + 6 - 3  # 9 initial + 6 new - 3 deletes
    assert "S3" not in df1["symbol"].unique().tolist()  # deleted symbol
    assert (df1["from"] == "source").sum() == 9  # 6 new + 3 updates

    # Cleanup
    shutil.rmtree(path)


def test_out_of_sequence():

    path, _ = build_target()

    # Test out-of-sequence
    dfs = spark.createDataFrame(pd.DataFrame({
        "symbol": ["S2", "S2"],
        "date": [datetime.date(2024, 12, 1), datetime.date(2024, 12, 1)],
        "close": [2.0, 1.0],
        "open": [2.0, 1.0],
        "from": ["source", "source"],
        "index": [2, 1],
    }))

    sink = models.FileDataSink(
        mode="MERGE",
        path=str(path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            exclude_columns=["index"],
            order_by="index",
        ),
    )
    sink.write(dfs)

    df1 = read(path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].to_dict()
    assert row == {'date': datetime.date(2024, 12, 1), 'symbol': 'S2', 'close': 2.0, 'open': 2.0, 'from': 'source'}

    # Cleanup
    shutil.rmtree(path)


def test_null_updates():

    path, _ = build_target()

    # Build Source Data
    dfs = spark.createDataFrame(data=[
        ("S2", datetime.date(2024, 11, 3), None, 2.0, "source")
    ], schema=T.StructType([
        T.StructField("symbol", T.StringType(), True),
        T.StructField("date", T.DateType(), True),
        T.StructField("close", T.DoubleType(), True),
        T.StructField("open", T.DoubleType(), True),
        T.StructField("from", T.StringType(), True)
    ]))

    # Update target without ignoring null updates
    sink = models.FileDataSink(
        mode="MERGE",
        path=str(path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
        ),
    )
    dfs.show()
    sink.write(dfs)

    # Test
    df1 = read(path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].fillna(-1).to_dict()
    assert row == {'date': datetime.date(2024, 11, 3), 'symbol': 'S2', 'close': -1, 'open': 2.0, 'from': 'source'}

    # Reset target
    _ = build_target(path)

    # Ignore null updates
    sink.merge_cdc_options.ignore_null_updates = True
    sink.write(dfs)

    # Test
    df1 = read(path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].to_dict()
    assert row == {'date': datetime.date(2024, 11, 3), 'symbol': 'S2', 'close': 0.0, 'open': 2.0, 'from': 'source'}

    # Cleanup
    shutil.rmtree(path)


if __name__ == "__main__":
    test_basic()
    test_out_of_sequence()
    test_null_updates()
