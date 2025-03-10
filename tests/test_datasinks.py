import os
import shutil
import uuid

import pyspark.sql.functions as F
import pytest
from pyspark.errors import AnalysisException
from pyspark.errors import IllegalArgumentException

from laktory._testing import Paths
from laktory._testing import dff
from laktory._testing import sparkf
from laktory.models import FileDataSink
from laktory.models import TableDataSink

paths = Paths(__file__)
spark = sparkf.spark


def test_file_data_sink_parquet():
    dirpath = paths.tmp / "df_slv_sink_parquet/"
    if dirpath.exists():
        shutil.rmtree(dirpath)

    # Write as overwrite
    sink = FileDataSink(
        path=dirpath,
        format="PARQUET",
        mode="OVERWRITE",
    )
    sink.write(dff.slv)

    # Write as append
    sink.write(dff.slv, mode="append")

    # Write and raise error
    with pytest.raises(AnalysisException):
        sink.write(dff.slv, mode="error")

    # Read back
    df = sink.as_source().read(spark=spark)

    # Test
    assert df.count() == 2 * dff.slv.count()

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)


def test_file_data_sink_delta():
    dirpath = paths.tmp / "df_slv_sink_delta/"
    if dirpath.exists():
        shutil.rmtree(dirpath)

    # Write as overwrite
    sink = FileDataSink(
        path=dirpath,
        format="DELTA",
        mode="OVERWRITE",
    )
    sink.write(dff.slv)

    # Write as append
    sink.write(dff.slv, mode="append")

    # Write and raise error
    with pytest.raises(AnalysisException):
        sink.write(dff.slv, mode="error")

    # Read back
    df = sink.as_source().read(spark=spark)

    # Test
    assert df.count() == 2 * dff.slv.count()

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)


def test_file_data_sink_stream():
    dirpath = paths.tmp / "df_slv_sink_stream/"
    if dirpath.exists():
        shutil.rmtree(dirpath)

    # Write
    sink = FileDataSink(
        path=dirpath,
        checkpoint_location=str(paths.tmp / "df_slv_sink_stream/checkpoint/"),
        format="DELTA",
        mode="APPEND",
    )
    sink.write(dff.slv_stream)

    # Write again
    # Should not add any new row because it's a stream and
    # source has not changed
    sink.write(dff.slv_stream, mode="append")

    # Write and raise error
    with pytest.raises(IllegalArgumentException):
        sink.write(dff.slv_stream, mode="error")

    # Read back
    df = sink.as_source().read(spark=spark)

    # Test
    assert df.count() == dff.slv.count()
    assert str(sink._checkpoint_location).endswith("tmp/df_slv_sink_stream/checkpoint")

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)
    assert not os.path.exists(sink._checkpoint_location)


def test_file_data_sink_stream_aggregate():
    dirpath = paths.tmp / "df_slv_sink_stream_aggregate/"
    if dirpath.exists():
        shutil.rmtree(dirpath)

    w = F.window("created_at", "5 days")
    slv_stream = (
        dff.slv_stream.withWatermark("created_at", "5 minutes")
        .groupby(w, "symbol")
        .agg(F.max("open").alias("high"))
    )
    count = dff.slv.groupby(w, "symbol").agg(F.max("symbol")).count()

    # Write
    sink = FileDataSink(
        path=dirpath,
        checkpoint_location=str(dirpath / "checkpoint/"),
        format="DELTA",
        mode="COMPLETE",
    )
    sink.write(slv_stream)

    # Write again
    # Should not add any new row because it's a stream and
    # source has not changed
    sink.write(slv_stream)

    # Write and raise error
    with pytest.raises(IllegalArgumentException):
        sink.write(slv_stream, mode="error")

    # Read back
    df = sink.as_source().read(spark=spark)

    # Test
    assert df.count() == count
    assert str(sink._checkpoint_location).endswith(
        "tmp/df_slv_sink_stream_aggregate/checkpoint"
    )

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)
    assert not os.path.exists(sink._checkpoint_location)


def test_file_data_sink_polars_parquet():
    filepath = paths.tmp / "dff.slv_polars_sink.parquet"

    if filepath.exists():
        os.remove(filepath)

    # Write parquet
    sink = FileDataSink(
        path=str(filepath),
        format="PARQUET",
    )
    sink.write(dff.slv_polars)

    # Write as append
    with pytest.raises(ValueError):
        sink.write(dff.slv_polars, mode="append")

    # Read back
    source = sink.as_source()
    source.dataframe_backend = "POLARS"
    df = source.read(filepath).collect()

    # Test
    assert df.height == dff.slv.count()
    assert df.columns == dff.slv.columns

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)


def test_file_data_sink_polars_delta():
    dirpath = paths.tmp / "df_slv_polars_sink.delta"
    if dirpath.exists():
        shutil.rmtree(dirpath)

    # Write as overwrite
    sink = FileDataSink(
        path=dirpath,
        format="DELTA",
        mode="OVERWRITE",
    )
    sink.write(dff.slv_polars)

    # Write as append
    sink.write(dff.slv_polars, mode="append")

    # Read back
    source = sink.as_source()
    source.dataframe_backend = "POLARS"
    df = source.read().collect()

    # Test
    assert df.height == dff.slv.count() * 2
    assert df.columns == dff.slv.columns

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)


def test_table_data_sink():
    # Write as overwrite
    sink = TableDataSink(
        schema_name="default",
        table_name="slv_stock_prices_sink",
        mode="OVERWRITE",
        write_options={
            "path": (
                paths.tmp / "hive" / f"slv_stock_prices_sink_{str(uuid.uuid4())}"
            ).as_posix(),
        },
    )
    assert sink._id == "default.slv_stock_prices_sink"
    assert sink._uuid == "65e2c312-188a-f4e2-3d49-d9f15bec2956"
    assert sink.full_name == "default.slv_stock_prices_sink"
    assert sink.format == "DELTA"
    assert sink.mode == "OVERWRITE"

    # Write
    sink.write(dff.slv)

    # Read back
    df = sink.as_source().read(spark=spark)

    # Test
    assert df.count() == dff.slv.count()
    assert df.columns == dff.slv.columns

    # Cleanup
    sink.purge(spark=spark)


def test_view_data_sink():
    # Create table
    table_path = paths.tmp / "hive" / f"slv_{str(uuid.uuid4())}"
    (
        dff.slv.write.mode("OVERWRITE")
        .option("path", table_path)
        .saveAsTable("default.slv")
    )

    # View Definition
    vd = "SELECT * FROM default.slv WHERE symbol = 'AAPL'"

    # Create Sinks
    sink1 = TableDataSink(
        schema_name="default",
        table_name="slv_aapl",
        table_type="VIEW",
        write_options={
            "path": (paths.tmp / "hive" / f"slv_aapl_{str(uuid.uuid4())}").as_posix(),
        },
    )
    sink2 = TableDataSink(
        schema_name="default",
        table_name="slv_aapl2",
        view_definition=vd,
    )

    # Write
    sink1.write(view_definition=vd, spark=spark)
    sink2.write(spark=spark)

    # Read back
    df1 = sink1.as_source().read(spark=spark)
    df2 = sink2.as_source().read(spark=spark)

    # Read Metastore
    df_tables = spark.sql("SHOW TABLES").toPandas()

    # Test
    assert sink1._id == "default.slv_aapl"
    assert sink1._uuid == "c8bc9f07-7611-cb73-c3b1-3cb69a5f3eb0"
    assert sink1.full_name == "default.slv_aapl"
    assert sink1.table_type == "VIEW"
    assert sink2._id == "default.slv_aapl2"
    assert sink2.full_name == "default.slv_aapl2"
    assert sink2.table_type == "VIEW"

    # Read Back
    _df = dff.slv.filter(F.col("symbol") == "AAPL")
    assert df1.count() == _df.count()
    assert df1.columns == _df.columns
    assert df2.count() == _df.count()
    assert df2.columns == _df.columns
    assert "slv_aapl" in df_tables["tableName"].to_list()
    assert "slv_aapl2" in df_tables["tableName"].to_list()

    # Cleanup
    sink1.purge(spark=spark)
    sink2.purge(spark=spark)
    if os.path.exists(table_path):
        shutil.rmtree(table_path)


if __name__ == "__main__":
    test_file_data_sink_parquet()
    test_file_data_sink_delta()
    test_file_data_sink_stream()
    test_file_data_sink_stream_aggregate()
    test_file_data_sink_polars_parquet()
    test_file_data_sink_polars_delta()
    test_table_data_sink()
    test_view_data_sink()
