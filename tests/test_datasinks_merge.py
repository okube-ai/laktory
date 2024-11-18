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


def build_target(write_target=True, path=None, with_index=False):

    if path is None:
        path = testdir_path / "tmp" / "test_datasinks_merge" / str(uuid.uuid4())

    df0 = pd.DataFrame([
        {"date": "2024-11-01", "symbol": "S0", "close": 0.42, "open": 0.16},
        {"date": "2024-11-01", "symbol": "S1", "close": 0.50, "open": 0.97},
        {"date": "2024-11-01", "symbol": "S2", "close": 0.47, "open": 0.60},
        {"date": "2024-11-02", "symbol": "S0", "close": 0.49, "open": 0.57},
        {"date": "2024-11-02", "symbol": "S1", "close": 0.45, "open": 0.14},
        {"date": "2024-11-02", "symbol": "S2", "close": 0.21, "open": 0.67},
        {"date": "2024-11-03", "symbol": "S0", "close": 0.97, "open": 0.09},
        {"date": "2024-11-03", "symbol": "S1", "close": 0.96, "open": 0.57},
        {"date": "2024-11-03", "symbol": "S2", "close": 0.00, "open": 0.93},
    ])
    df0 = spark.createDataFrame(df0)
    df0 = df0.withColumn("date", F.col("date").cast("date"))
    df0 = df0.withColumn("_is_deleted", F.lit(False))
    df0 = df0.withColumn("from", F.lit("target"))
    if with_index:
        df0 = df0.withColumn("index", F.lit(1))
    # _df0.printSchema()
    #
    # print(df0)
    # print()
    #
    #
    # nstocks = 3
    # nstamps = 5
    #
    # # Timestamps
    # t0 = datetime.date(2024, 11, 1)
    # dates = [t0 + datetime.timedelta(days=i) for i in range(nstamps)]
    #
    # # Symbols
    # symbols = [f"S{i:1d}" for i in range(nstocks)]
    #
    # # Create an empty DataFrame with the schema
    # df_symbols = spark.createDataFrame(pd.DataFrame({"symbol": symbols}))
    # df_dates = spark.createDataFrame(pd.DataFrame({"date": dates}))
    # df = df_dates.crossJoin(df_symbols)
    #
    # # Add columns with random numbers
    # for i, c in enumerate(price_cols):
    #     df = df.withColumn(c, F.round(F.rand(seed=i), 2))
    #
    # df = df.withColumn("from", F.lit("target"))
    # df = df.withColumn("index", F.lit(1))
    # df = df.sort("date", "symbol")
    #
    # first_symbols = F.col("symbol").isin([f"S{i:1d}" for i in [0]])
    # last_symbols = F.col("symbol").isin([f"S{i:1d}" for i in [nstocks - 1]])
    #
    # # Last 2 timestamps are new rows
    # # First symbol will be updated
    # # Last symbol will be deleted
    # is_new = F.col("date") >= dates[-2]
    # df = df.withColumn("_is_new", is_new)
    # df = df.withColumn("_is_updated", first_symbols & ~is_new)
    # df = df.withColumn("_is_deleted", last_symbols & ~is_new)

    # Build Target
    # df0 = df.filter(~F.col("_is_new")).drop("_is_updated", "_is_new")
    if write_target:
        (
            df0
            .drop("_is_deleted")   # would be dropped by the merge function
            .write.format("DELTA")
            .mode("OVERWRITE")
            .save(str(path))
        )
    #
    # # Build Source
    # c1 = F.col("_is_new")
    # c2 = F.col("_is_updated")
    # c3 = F.col("_is_deleted")
    #
    # dfs = df.filter(c1 | c2 | c3)
    # dfs = dfs.drop("_is_new", "_is_updated")
    # dfs = dfs.withColumn("from", F.lit("source"))

    return path, df0  #, dfs


def read(path):
    return spark.read.format("DELTA").load(str(path))


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #

def test_basic():

    path, df = build_target(write_target=False)

    # Write target
    sink = models.FileDataSink(
        mode="MERGE",
        path=str(path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            delete_where="source._is_deleted = true",
            exclude_columns=["_is_deleted"]
        ),
    )
    sink.write(df)

    # Test target
    df0 = read(path).toPandas()
    assert len(df0) == 9  # 3 stocks * 3 timestamps
    assert df0["from"].unique().tolist() == ["target"]
    assert df0.columns.tolist() == ['symbol', 'date', 'close', 'open', 'from']

    # Build Source
    dfs = pd.DataFrame([
        {"date": "2024-11-01", "symbol": "S0", "close": 0.42, "open": 0.16, "_is_deleted": False},
        {"date": "2024-11-01", "symbol": "S2", "close": 0.47, "open": 0.60, "_is_deleted":  True},
        {"date": "2024-11-02", "symbol": "S0", "close": 0.49, "open": 0.57, "_is_deleted": False},
        {"date": "2024-11-02", "symbol": "S2", "close": 0.21, "open": 0.67, "_is_deleted":  True},
        {"date": "2024-11-03", "symbol": "S0", "close": 0.97, "open": 0.09, "_is_deleted": False},
        {"date": "2024-11-03", "symbol": "S2", "close": 0.00, "open": 0.93, "_is_deleted":  True},
        {"date": "2024-11-04", "symbol": "S0", "close": 0.36, "open": 0.29, "_is_deleted": False},
        {"date": "2024-11-04", "symbol": "S1", "close": 0.39, "open": 0.25, "_is_deleted": False},
        {"date": "2024-11-04", "symbol": "S2", "close": 0.48, "open": 0.06, "_is_deleted": False},
        {"date": "2024-11-05", "symbol": "S0", "close": 0.80, "open": 0.33, "_is_deleted": False},
        {"date": "2024-11-05", "symbol": "S1", "close": 0.86, "open": 0.50, "_is_deleted": False},
        {"date": "2024-11-05", "symbol": "S2", "close": 0.73, "open": 0.51, "_is_deleted": False},
    ])
    dfs = spark.createDataFrame(dfs)
    dfs = dfs.withColumn("date", F.col("date").cast("date"))
    dfs = dfs.withColumn("from", F.lit("source"))

    # Merge source
    sink.write(dfs)

    # Read updated target
    df1 = read(path).sort("date", "symbol").toPandas()

    # Test Merge
    assert df1.columns.tolist() == ['symbol', 'date', 'close', 'open', 'from']
    assert len(df1) == 9 + 6 - 3  # 9 initial + 6 new - 3 deletes
    assert "S3" not in df1["symbol"].unique().tolist()  # deleted symbol
    assert (df1["from"] == "source").sum() == 9  # 6 new + 3 updates

    # Cleanup
    shutil.rmtree(path)


def test_out_of_sequence():

    path, df0 = build_target(with_index=False)

    # Out-of-sequence source
    dfs = spark.createDataFrame(pd.DataFrame({
        "symbol": ["S2", "S2"],
        "date": [datetime.date(2024, 12, 1), datetime.date(2024, 12, 1)],
        "close": [2.0, 1.0],
        "open": [2.0, 1.0],
        "from": ["source", "source"],
        "index": [2, 1],
    }))

    # Merge
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

    # Test
    df1 = read(path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].to_dict()
    assert row == {'date': datetime.date(2024, 12, 1), 'symbol': 'S2', 'close': 2.0, 'open': 2.0, 'from': 'source'}

    # Cleanup
    shutil.rmtree(path)


def test_scd_type_2():

    path, _ = build_target(add_index=True)

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
            scd_type=2,
        ),
    )
    sink.write(dfs)

    df1 = read(path).sort("date", "symbol").toPandas()
    print(df1)
    # row = df1.iloc[-1].to_dict()
    # assert row == {'date': datetime.date(2024, 12, 1), 'symbol': 'S2', 'close': 2.0, 'open': 2.0, 'from': 'source'}
    #
    # # Cleanup
    # shutil.rmtree(path)


def test_null_updates():

    path, _ = build_target()

    # Build Source Data
    dfs = pd.DataFrame([
        {"date": "2024-11-03", "symbol": "S2", "close": 0.0, "open": 2.00},
    ])
    dfs = spark.createDataFrame(dfs)
    dfs = dfs.withColumn("date", F.col("date").cast("date"))
    dfs = dfs.withColumn("close", F.lit(None))
    dfs = dfs.withColumn("from", F.lit("source"))

    # Update target without ignoring null updates
    sink = models.FileDataSink(
        mode="MERGE",
        path=str(path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
        ),
    )
    sink.write(dfs)

    # Test
    df1 = read(path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].fillna(-1).to_dict()
    assert row == {'date': datetime.date(2024, 11, 3), 'symbol': 'S2', 'close': -1, 'open': 2.0, 'from': 'source'}

    # Reset target
    _ = build_target(path=path)

    # Ignore null updates
    sink.merge_cdc_options.ignore_null_updates = True
    sink.write(dfs)

    # Test
    df1 = read(path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].to_dict()
    assert row == {'date': datetime.date(2024, 11, 3), 'symbol': 'S2', 'close': 0.0, 'open': 2.0, 'from': 'source'}

    # Cleanup
    shutil.rmtree(path)


def test_stream():

    path, _ = build_target()

    # Build Source
    dfs = pd.DataFrame([
        {"date": "2024-11-01", "symbol": "S0", "close": 0.42, "open": 0.16, "_is_deleted": False},
        {"date": "2024-11-01", "symbol": "S2", "close": 0.47, "open": 0.60, "_is_deleted":  True},
        {"date": "2024-11-02", "symbol": "S0", "close": 0.49, "open": 0.57, "_is_deleted": False},
        {"date": "2024-11-02", "symbol": "S2", "close": 0.21, "open": 0.67, "_is_deleted":  True},
        {"date": "2024-11-03", "symbol": "S0", "close": 0.97, "open": 0.09, "_is_deleted": False},
        {"date": "2024-11-03", "symbol": "S2", "close": 0.00, "open": 0.93, "_is_deleted":  True},
        {"date": "2024-11-04", "symbol": "S0", "close": 0.36, "open": 0.29, "_is_deleted": False},
        {"date": "2024-11-04", "symbol": "S1", "close": 0.39, "open": 0.25, "_is_deleted": False},
        {"date": "2024-11-04", "symbol": "S2", "close": 0.48, "open": 0.06, "_is_deleted": False},
        {"date": "2024-11-05", "symbol": "S0", "close": 0.80, "open": 0.33, "_is_deleted": False},
        {"date": "2024-11-05", "symbol": "S1", "close": 0.86, "open": 0.50, "_is_deleted": False},
        {"date": "2024-11-05", "symbol": "S2", "close": 0.73, "open": 0.51, "_is_deleted": False},
    ])
    dfs = spark.createDataFrame(dfs)
    dfs = dfs.withColumn("date", F.col("date").cast("date"))
    dfs = dfs.withColumn("from", F.lit("source"))

    # Convert source to stream
    source_path = str(testdir_path / "tmp" / "test_datasinks_merge" / str(uuid.uuid4()))
    dfs.write.format("DELTA").mode("overwrite").save(source_path)
    dfs = spark.readStream.format("DELTA").load(source_path)

    # Create sink and write
    sink = models.FileDataSink(
        mode="MERGE",
        checkpoint_location=str(path),
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
    print(df1)
    assert df1.columns.tolist() == ['date', 'symbol', 'close', 'open', 'from']
    assert len(df1) == 9 + 6 - 3  # 9 initial + 6 new - 3 deletes
    assert "S3" not in df1["symbol"].unique().tolist()  # deleted symbol
    assert (df1["from"] == "source").sum() == 9  # 6 new + 3 updates

    # Cleanup
    shutil.rmtree(path)
    shutil.rmtree(source_path)


if __name__ == "__main__":
    test_basic()
    test_out_of_sequence()
    # dfs = test_scd_type_2()
    test_null_updates()
    test_stream()
