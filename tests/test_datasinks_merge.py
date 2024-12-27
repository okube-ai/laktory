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


def build_target(write_target=True, path=None, index=None):

    if path is None:
        path = testdir_path / "tmp" / "test_datasinks_merge" / str(uuid.uuid4())

    # Build Target
    df0 = pd.DataFrame(
        [
            {"date": "2024-11-01", "symbol": "S0", "close": 0.42, "open": 0.16},
            {"date": "2024-11-01", "symbol": "S1", "close": 0.50, "open": 0.97},
            {"date": "2024-11-01", "symbol": "S2", "close": 0.47, "open": 0.60},
            {"date": "2024-11-02", "symbol": "S0", "close": 0.49, "open": 0.57},
            {"date": "2024-11-02", "symbol": "S1", "close": 0.45, "open": 0.14},
            {"date": "2024-11-02", "symbol": "S2", "close": 0.21, "open": 0.67},
            {"date": "2024-11-03", "symbol": "S0", "close": 0.97, "open": 0.09},
            {"date": "2024-11-03", "symbol": "S1", "close": 0.96, "open": 0.57},
            {"date": "2024-11-03", "symbol": "S2", "close": 0.00, "open": 0.93},
        ]
    )
    df0 = spark.createDataFrame(df0)
    df0 = df0.withColumn("date", F.col("date").cast("date"))
    df0 = df0.withColumn("_is_deleted", F.lit(False))
    df0 = df0.withColumn("from", F.lit("target"))
    if index:
        df0 = df0.withColumn("index", F.lit(index))

    # Write Target
    if write_target:

        (
            df0.withColumn(
                "__hash_keys", F.lit(F.sha2(F.concat_ws("~", *["symbol", "date"]), 256))
            )
            .drop("_is_deleted")  # would be dropped by the merge function
            .write.format("DELTA")
            .mode("OVERWRITE")
            .save(str(path))
        )

    return path, df0


def get_basic_source():

    dfs = pd.DataFrame(
        [
            # Delete
            {
                "date": "2024-11-01",
                "symbol": "S2",
                "close": 0.47,
                "open": 0.60,
                "_is_deleted": True,
            },
            # Delete
            {
                "date": "2024-11-02",
                "symbol": "S2",
                "close": 0.21,
                "open": 0.67,
                "_is_deleted": True,
            },
            # Update
            {
                "date": "2024-11-03",
                "symbol": "S0",
                "close": 0.97,
                "open": 0.09,
                "_is_deleted": False,
            },
            # Delete
            {
                "date": "2024-11-03",
                "symbol": "S2",
                "close": 0.00,
                "open": 0.93,
                "_is_deleted": True,
            },
            # Insert
            {
                "date": "2024-11-04",
                "symbol": "S0",
                "close": 0.36,
                "open": 0.29,
                "_is_deleted": False,
            },
            {
                "date": "2024-11-04",
                "symbol": "S1",
                "close": 0.39,
                "open": 0.25,
                "_is_deleted": False,
            },
            {
                "date": "2024-11-04",
                "symbol": "S2",
                "close": 0.48,
                "open": 0.06,
                "_is_deleted": False,
            },
            {
                "date": "2024-11-05",
                "symbol": "S0",
                "close": 0.80,
                "open": 0.33,
                "_is_deleted": False,
            },
            {
                "date": "2024-11-05",
                "symbol": "S1",
                "close": 0.86,
                "open": 0.50,
                "_is_deleted": False,
            },
            {
                "date": "2024-11-05",
                "symbol": "S2",
                "close": 0.73,
                "open": 0.51,
                "_is_deleted": False,
            },
        ]
    )
    dfs = spark.createDataFrame(dfs)
    dfs = dfs.withColumn("date", F.col("date").cast("date"))
    dfs = dfs.withColumn("from", F.lit("source"))

    return dfs


def get_scd2_source():
    dfs = pd.DataFrame(
        [
            {
                "date": "2024-11-03",
                "symbol": "S0",
                "close": 0.97,
                "open": 0.09,
                "index": 2,
                "_is_deleted": True,
            },  # duplicate - to be deleted
            {
                "date": "2024-11-03",
                "symbol": "S1",
                "close": 0.96,
                "open": 0.57,
                "index": 1,
                "_is_deleted": False,
            },  # duplicate, should be ignored
            {
                "date": "2024-11-03",
                "symbol": "S2",
                "close": 3.0,
                "open": 3.00,
                "index": 3,
                "_is_deleted": False,
            },
            {
                "date": "2024-11-03",
                "symbol": "S2",
                "close": 4.0,
                "open": 4.00,
                "index": 4,
                "_is_deleted": False,
            },
            {
                "date": "2024-11-03",
                "symbol": "S2",
                "close": 2.0,
                "open": 2.00,
                "index": 2,
                "_is_deleted": False,
            },
        ]
    )
    dfs = spark.createDataFrame(dfs)
    dfs = dfs.withColumn("date", F.col("date").cast("date"))
    dfs = dfs.withColumn("index", F.col("index").cast(T.IntegerType()))

    return dfs


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
            exclude_columns=["_is_deleted"],
        ),
    )
    assert sink.is_cdc
    sink.write(df)

    # Test target
    df0 = read(path).toPandas()
    assert len(df0) == 9  # 3 stocks * 3 timestamps
    assert df0["from"].unique().tolist() == ["target"]
    assert df0.columns.tolist() == [
        "symbol",
        "date",
        "close",
        "open",
        "from",
        "__hash_keys",
    ]

    # Build Source
    dfs = get_basic_source()

    # Merge source
    sink.write(dfs)

    # Read updated target
    df1 = read(path).sort("date", "symbol").toPandas()

    # Test Merge
    assert df1.columns.tolist() == [
        "symbol",
        "date",
        "close",
        "open",
        "from",
        "__hash_keys",
    ]
    assert len(df1) == 9 + 6 - 3  # 9 initial + 6 new - 3 deletes
    assert "S3" not in df1["symbol"].unique().tolist()  # deleted symbol
    assert (df1["from"] == "source").sum() == 7  # 6 new + 1 updates

    # Cleanup
    shutil.rmtree(path)


def test_out_of_sequence():

    path, df0 = build_target(index=1)

    # Out-of-sequence source
    dfs = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol": ["S2", "S2"],
                "date": [datetime.date(2024, 12, 1), datetime.date(2024, 12, 1)],
                "close": [2.0, 1.0],
                "open": [2.0, 1.0],
                "from": ["source", "source"],
                "index": [2, 1],
            }
        )
    )

    # Merge
    sink = models.FileDataSink(
        mode="MERGE",
        path=str(path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            order_by="index",
        ),
    )
    sink.write(dfs)

    # Test
    df1 = read(path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].to_dict()
    del row["__hash_keys"]
    assert row == {
        "date": datetime.date(2024, 12, 1),
        "symbol": "S2",
        "close": 2.0,
        "open": 2.0,
        "from": "source",
        "index": 2,
    }

    # Cleanup
    shutil.rmtree(path)


def test_outdated():

    path, df0 = build_target(write_target=True, index=3)

    # Out-of-sequence source
    dfs = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol": ["S1", "S2"],
                "date": [datetime.date(2024, 11, 3), datetime.date(2024, 11, 3)],
                "close": [4.0, 1.0],
                "open": [4.0, 1.0],
                "from": ["source", "source"],
                "index": [4, 1],
            }
        )
    )

    # Merge
    sink = models.FileDataSink(
        mode="MERGE",
        path=str(path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            order_by="index",
        ),
    )
    sink.write(dfs)

    # Test - Updated Row
    df1 = read(path).sort("date", "symbol").toPandas()
    row = df1.iloc[-2].to_dict()
    del row["__hash_keys"]
    assert row == {
        "date": datetime.date(2024, 11, 3),
        "symbol": "S1",
        "close": 4.0,
        "open": 4.0,
        "from": "source",
        "index": 4,
    }

    # Test - Non-updated Row
    row = df1.iloc[-3].to_dict()
    del row["__hash_keys"]
    assert row == {
        "date": datetime.date(2024, 11, 3),
        "symbol": "S0",
        "close": 0.97,
        "open": 0.09,
        "from": "target",
        "index": 3,
    }

    # Cleanup
    shutil.rmtree(path)


def test_delete_non_existent():

    path, df0 = build_target(index=1)

    # Source with rows "pre-deleted"
    dfs = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol": ["S2", "S1"],
                "date": [datetime.date(2024, 12, 1), datetime.date(2024, 12, 1)],
                "close": [2.0, 1.0],
                "open": [2.0, 1.0],
                "from": ["source", "source"],
                "index": [1, 1],
                "_is_deleted": [True, True],
            }
        )
    )

    # Merge
    sink = models.FileDataSink(
        mode="MERGE",
        path=str(path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            delete_where="_is_deleted = true",
            exclude_columns=["_is_deleted"],
        ),
    )
    sink.write(dfs)

    # Test
    df1 = read(path).sort("date", "symbol").toPandas()
    assert len(df1) == df0.count()

    # Cleanup
    shutil.rmtree(path)


def test_scd2():

    path, df = build_target(write_target=False, index=1)

    # Build Source Data
    dfs = get_scd2_source()

    # Merge
    sink = models.FileDataSink(
        mode="MERGE",
        path=str(path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            exclude_columns=["_is_deleted"],
            delete_where="_is_deleted = true",
            order_by="index",
            scd_type=2,
            start_at_column_name="start_at",
        ),
    )
    sink.write(df.drop("from"))
    sink.write(dfs)

    # Test
    df1 = read(path).sort("date", "symbol", "start_at").toPandas()
    where = (df1["symbol"] == "S2") & (df1["date"] == datetime.date(2024, 11, 3))
    assert len(df1) == df.count() + 3  # 3 updates | 1 delete does not add new row
    assert df1["__end_at"].count() == 4  # 3 updates + 1 delete
    assert df1.loc[where]["__end_at"].fillna(-1).tolist() == [2, 3, 4, -1]

    # Cleanup
    shutil.rmtree(path)


def test_null_updates():

    path, _ = build_target()

    # Build Source Data
    dfs = pd.DataFrame(
        [
            {"date": "2024-11-03", "symbol": "S2", "close": 0.0, "open": 2.00},
        ]
    )
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
    del row["__hash_keys"]
    assert row == {
        "date": datetime.date(2024, 11, 3),
        "symbol": "S2",
        "close": -1,
        "open": 2.0,
        "from": "source",
    }

    # Reset target
    _ = build_target(path=path)

    # Ignore null updates
    sink.merge_cdc_options.ignore_null_updates = True
    sink.write(dfs)

    # Test
    df1 = read(path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].to_dict()
    del row["__hash_keys"]
    assert row == {
        "date": datetime.date(2024, 11, 3),
        "symbol": "S2",
        "close": 0.0,
        "open": 2.0,
        "from": "source",
    }

    # Cleanup
    shutil.rmtree(path)


def test_stream():

    path, _ = build_target()

    # Build Source
    dfs = get_basic_source()

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
    assert df1.columns.tolist() == [
        "date",
        "symbol",
        "close",
        "open",
        "from",
        "__hash_keys",
    ]
    assert len(df1) == 9 + 6 - 3  # 9 initial + 6 new - 3 deletes
    assert "S3" not in df1["symbol"].unique().tolist()  # deleted symbol
    assert (df1["from"] == "source").sum() == 7  # 6 new + 1 updates

    # Cleanup
    shutil.rmtree(path)
    shutil.rmtree(source_path)


def test_stream_scd2():

    path, df = build_target(write_target=False, index=1)

    # Build Source
    dfs = get_scd2_source()

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
            exclude_columns=["_is_deleted"],
            delete_where="_is_deleted = true",
            order_by="index",
            scd_type=2,
            start_at_column_name="start_at",
        ),
    )
    sink.write(df.drop("from"))
    sink.write(dfs)

    # Tests
    df1 = read(path).sort("date", "symbol", "start_at").toPandas()
    where = (df1["symbol"] == "S2") & (df1["date"] == datetime.date(2024, 11, 3))
    assert len(df1) == df.count() + 3  # 3 updates | 1 delete does not add new row
    assert df1["__end_at"].count() == 4  # 3 updates + 1 delete
    assert df1.loc[where]["__end_at"].fillna(-1).tolist() == [2, 3, 4, -1]

    # Cleanup
    shutil.rmtree(path)
    shutil.rmtree(source_path)


def test_dlt_kwargs():
    sink = models.TableDataSink(
        mode="MERGE",
        checkpoint_location="root/table/checkpoint",
        table_name="my_table",
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            exclude_columns=["_is_deleted"],
            delete_where="_is_deleted = true",
            order_by="index",
            scd_type=2,
            start_at_column_name="start_at",
        ),
    )

    assert sink.dlt_apply_changes_kwargs == {
        "apply_as_deletes": "_is_deleted = true",
        "column_list": None,
        "except_column_list": ["_is_deleted"],
        "ignore_null_updates": False,
        "keys": ["symbol", "date"],
        "sequence_by": "index",
        "stored_as_scd_type": 2,
        "target": "my_table",
    }


if __name__ == "__main__":
    # test_basic()
    # test_out_of_sequence()
    # test_outdated()
    test_delete_non_existent()
    # test_scd2()
    # test_null_updates()
    # test_stream()
    # test_dlt_kwargs()
