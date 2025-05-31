from __future__ import annotations

import datetime

import narwhals as nw
import pandas as pd
import polars as pl
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest

import laktory
from laktory import models
from laktory.enums import DataFrameBackends
from laktory.models.datasinks.mergecdcoptions import SUPPORTED_BACKENDS

spark = laktory.get_spark_session()

# --------------------------------------------------------------------------- #
# Functions                                                                   #
# --------------------------------------------------------------------------- #


price_cols = ["close", "open"]


def build_target(path, backend, write_target=True, index=None) -> nw.LazyFrame:
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
    if backend == "PYSPARK":
        df0 = spark.createDataFrame(df0)
        df0 = df0.withColumn("date", F.col("date").cast("date"))
        df0 = df0.withColumn("_is_deleted", F.lit(False))
        df0 = df0.withColumn("from", F.lit("target"))
        if index:
            df0 = df0.withColumn("index", F.lit(index))
    elif backend == "POLARS":
        df0 = pl.from_pandas(df0)
        # df0 = df0.with_columns({
        #     "date": pl.col("date").cast(pl.Date()),
        #     "_is_deleted": pl.lit(False),
        #     "from": pl.lit("target"),
        # })

    # Write Target
    if write_target:
        (
            df0.drop("_is_deleted")  # would be dropped by the merge function
            .write.format("DELTA")
            .mode("OVERWRITE")
            .save(str(path))
        )

    return nw.from_native(df0)


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


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_basic(tmp_path, backend):
    df = build_target(path=tmp_path, write_target=False, backend=backend)

    # Write target
    sink = models.FileDataSink(
        format="DELTA",
        mode="MERGE",
        path=str(tmp_path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            delete_where="source._is_deleted = true",
            exclude_columns=["_is_deleted"],
        ),
    )
    assert sink.is_cdc

    if DataFrameBackends(backend) not in SUPPORTED_BACKENDS:
        with pytest.raises(NotImplementedError):
            sink.write(df)
        return

    sink.write(df)

    # Test target
    df0 = read(tmp_path).toPandas()
    assert len(df0) == 9  # 3 stocks * 3 timestamps
    assert df0["from"].unique().tolist() == ["target"]
    assert df0.columns.tolist() == [
        "symbol",
        "date",
        "close",
        "open",
        "from",
    ]

    # Build Source
    dfs = get_basic_source()

    # Merge source
    sink.write(dfs)

    # Read updated target
    df1 = read(tmp_path).sort("date", "symbol").toPandas()

    # Test Merge
    assert df1.columns.tolist() == [
        "symbol",
        "date",
        "close",
        "open",
        "from",
    ]
    assert len(df1) == 9 + 6 - 3  # 9 initial + 6 new - 3 deletes
    assert "S3" not in df1["symbol"].unique().tolist()  # deleted symbol
    assert (df1["from"] == "source").sum() == 7  # 6 new + 1 updates


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_out_of_sequence(tmp_path, backend):
    if DataFrameBackends(backend) not in SUPPORTED_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    build_target(tmp_path, backend=backend, index=1)

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
        format="DELTA",
        path=str(tmp_path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            order_by="index",
        ),
    )
    sink.write(dfs)

    # Test
    df1 = read(tmp_path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].to_dict()
    assert len(df1) == 10
    assert row == {
        "date": datetime.date(2024, 12, 1),
        "symbol": "S2",
        "close": 2.0,
        "open": 2.0,
        "from": "source",
        "index": 2,
    }


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_outdated(tmp_path, backend):
    if DataFrameBackends(backend) not in SUPPORTED_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    build_target(tmp_path, backend=backend, index=3)

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
        format="DELTA",
        path=str(tmp_path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            order_by="index",
        ),
    )
    sink.write(dfs)

    # Test - Updated Row
    df1 = read(tmp_path).sort("date", "symbol").toPandas()

    row = df1.iloc[-2].to_dict()
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
    assert row == {
        "date": datetime.date(2024, 11, 3),
        "symbol": "S0",
        "close": 0.97,
        "open": 0.09,
        "from": "target",
        "index": 3,
    }


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_delete_non_existent(tmp_path, backend):
    if DataFrameBackends(backend) not in SUPPORTED_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    df0 = build_target(path=tmp_path, backend=backend, index=1)

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
        format="DELTA",
        path=str(tmp_path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            delete_where="_is_deleted = true",
            exclude_columns=["_is_deleted"],
        ),
    )
    sink.write(dfs)

    # Test
    df1 = read(tmp_path).sort("date", "symbol").toPandas()
    assert len(df1) == df0.collect().shape[0]


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_scd2(tmp_path, backend):
    if DataFrameBackends(backend) not in SUPPORTED_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    df = build_target(path=tmp_path, backend=backend, write_target=False, index=1)

    # Build Source Data
    dfs = get_scd2_source()

    # Merge
    sink = models.FileDataSink(
        mode="MERGE",
        format="DELTA",
        path=str(tmp_path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            order_by="index",
            scd_type=2,
        ),
    )
    sink.write(df.drop("from"))
    sink.write(dfs)

    # Test
    df1 = read(tmp_path).sort("date", "symbol", "__start_at").toPandas()
    where = (df1["symbol"] == "S2") & (df1["date"] == datetime.date(2024, 11, 3))
    assert len(df1) == df.collect().shape[0] + dfs.count()
    assert df1["__end_at"].count() == dfs.count()
    assert df1.loc[where]["__end_at"].fillna(-1).tolist() == [2, 3, 4, -1]


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_scd2_with_delete(tmp_path, backend):
    if DataFrameBackends(backend) not in SUPPORTED_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    df = build_target(path=tmp_path, backend=backend, write_target=False, index=1)

    # Build Source Data
    dfs = get_scd2_source()

    # Merge
    sink = models.FileDataSink(
        mode="MERGE",
        path=str(tmp_path),
        format="DELTA",
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
    df1 = read(tmp_path).sort("date", "symbol", "start_at").toPandas()
    where = (df1["symbol"] == "S2") & (df1["date"] == datetime.date(2024, 11, 3))
    assert (
        len(df1) == df.collect().shape[0] + 3
    )  # 3 updates | 1 delete does not add new row
    assert df1["__end_at"].count() == 4  # 3 updates + 1 delete
    assert df1.loc[where]["__end_at"].fillna(-1).tolist() == [2, 3, 4, -1]


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_null_updates(tmp_path, backend):
    if DataFrameBackends(backend) not in SUPPORTED_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    build_target(path=tmp_path, backend=backend)

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
        path=str(tmp_path),
        format="DELTA",
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
        ),
    )
    sink.write(dfs)

    # Test
    df1 = read(tmp_path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].fillna(-1).to_dict()
    assert row == {
        "date": datetime.date(2024, 11, 3),
        "symbol": "S2",
        "close": -1,
        "open": 2.0,
        "from": "source",
    }

    # Reset target
    _ = build_target(path=tmp_path, backend=backend)

    # Ignore null updates
    sink.merge_cdc_options.ignore_null_updates = True
    sink.write(dfs)

    # Test
    df1 = read(tmp_path).sort("date", "symbol").toPandas()
    row = df1.iloc[-1].to_dict()
    assert row == {
        "date": datetime.date(2024, 11, 3),
        "symbol": "S2",
        "close": 0.0,
        "open": 2.0,
        "from": "source",
    }


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_stream(tmp_path, backend):
    if DataFrameBackends(backend) not in SUPPORTED_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    build_target(
        path=tmp_path,
        backend=backend,
    )

    # Build Source
    dfs = get_basic_source()

    # Convert source to stream
    source_path = str(tmp_path / "source")
    dfs.write.format("DELTA").mode("overwrite").save(source_path)
    dfs = spark.readStream.format("DELTA").load(source_path)

    # Create sink and write
    sink = models.FileDataSink(
        mode="MERGE",
        checkpoint_path=str(tmp_path),
        format="DELTA",
        path=str(tmp_path),
        merge_cdc_options=models.DataSinkMergeCDCOptions(
            primary_keys=["symbol", "date"],
            delete_where="source._is_deleted = true",
            exclude_columns=["_is_deleted"],
        ),
    )
    sink.write(dfs)

    # Read updated target
    df1 = read(tmp_path).sort("date", "symbol").toPandas()
    assert df1.columns.tolist() == [
        "date",
        "symbol",
        "close",
        "open",
        "from",
    ]
    assert len(df1) == 9 + 6 - 3  # 9 initial + 6 new - 3 deletes
    assert "S3" not in df1["symbol"].unique().tolist()  # deleted symbol
    assert (df1["from"] == "source").sum() == 7  # 6 new + 1 updates


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_stream_scd2(tmp_path, backend):
    if DataFrameBackends(backend) not in SUPPORTED_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    df = build_target(path=tmp_path, backend=backend, write_target=False, index=1)

    # Build Source
    dfs = get_scd2_source()

    # Convert source to stream
    source_path = str(tmp_path / "source")
    dfs.write.format("DELTA").mode("overwrite").save(source_path)
    dfs = spark.readStream.format("DELTA").load(source_path)

    # Create sink and write
    sink = models.FileDataSink(
        mode="MERGE",
        format="DELTA",
        checkpoint_path=str(tmp_path),
        path=str(tmp_path),
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
    df1 = read(tmp_path).sort("date", "symbol", "start_at").toPandas()
    where = (df1["symbol"] == "S2") & (df1["date"] == datetime.date(2024, 11, 3))
    assert (
        len(df1) == df.collect().shape[0] + 3
    )  # 3 updates | 1 delete does not add new row
    assert df1["__end_at"].count() == 4  # 3 updates + 1 delete
    assert df1.loc[where]["__end_at"].fillna(-1).tolist() == [2, 3, 4, -1]


@pytest.mark.parametrize("backend", ["PYSPARK", "POLARS"])
def test_dlt_kwargs(tmp_path, backend):
    if DataFrameBackends(backend) not in SUPPORTED_BACKENDS:
        pytest.skip(f"Backend '{backend}' not implemented.")

    sink = models.HiveMetastoreDataSink(
        mode="MERGE",
        format="DELTA",
        # checkpoint_location="root/table/checkpoint",
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
