import os
import shutil
import pytest
from pyspark.errors import AnalysisException
from pyspark.errors import IllegalArgumentException
import polars as pl

from laktory.models import TableDataSink
from laktory.models import FileDataSink
from laktory._testing import df_slv
from laktory._testing import df_slv_stream
from laktory._testing import df_slv_polars
from laktory._testing import Paths
from laktory._testing import spark

paths = Paths(__file__)


def test_file_data_sink_parquet():

    dirpath = os.path.join(paths.tmp, "df_slv_sink_parquet/")
    if os.path.exists(dirpath):
        shutil.rmtree(dirpath)

    # Write as overwrite
    sink = FileDataSink(
        path=dirpath,
        format="PARQUET",
        mode="OVERWRITE",
    )
    sink.write(df_slv)

    # Write as append
    sink.write(df_slv, mode="append")

    # Write and raise error
    with pytest.raises(AnalysisException):
        sink.write(df_slv, mode="error")

    # Read back
    df = sink.as_source().read(spark=spark)

    # Test
    assert df.count() == 2 * df_slv.count()

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)


def test_file_data_sink_delta():

    dirpath = os.path.join(paths.tmp, "df_slv_sink_delta/")
    if os.path.exists(dirpath):
        shutil.rmtree(dirpath)

    # Write as overwrite
    sink = FileDataSink(
        path=dirpath,
        format="DELTA",
        mode="OVERWRITE",
    )
    sink.write(df_slv)

    # Write as append
    sink.write(df_slv, mode="append")

    # Write and raise error
    with pytest.raises(AnalysisException):
        sink.write(df_slv, mode="error")

    # Read back
    df = sink.as_source().read(spark=spark)

    # Test
    assert df.count() == 2 * df_slv.count()

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)


def test_file_data_sink_stream():

    dirpath = os.path.join(paths.tmp, "df_slv_sink_stream/")
    if os.path.exists(dirpath):
        shutil.rmtree(dirpath)

    # Write
    sink = FileDataSink(
        path=dirpath,
        checkpoint_location=os.path.join(paths.tmp, "df_slv_sink_stream/checkpoint/"),
        format="DELTA",
        mode="APPEND",
    )
    sink.write(df_slv_stream)

    # Write again
    # Should not add any new row because it's a stream and
    # source has not changed
    sink.write(df_slv_stream, mode="append")

    # Write and raise error
    with pytest.raises(IllegalArgumentException):
        sink.write(df_slv_stream, mode="error")

    # Read back
    df = sink.as_source().read(spark=spark)

    # Test
    assert df.count() == df_slv.count()
    assert str(sink._checkpoint_location).endswith("tmp/df_slv_sink_stream/checkpoint")

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)
    assert not os.path.exists(sink._checkpoint_location)


def test_file_data_sink_polars_parquet():

    filepath = os.path.join(paths.tmp, "df_slv_polars_sink.parquet")

    if os.path.exists(filepath):
        os.remove(filepath)

    # Write parquet
    sink = FileDataSink(
        path=filepath,
        format="PARQUET",
    )
    sink.write(df_slv_polars)

    # Write as append
    with pytest.raises(ValueError):
        sink.write(df_slv_polars, mode="append")

    # Read back
    source = sink.as_source()
    source.dataframe_type = "POLARS"
    df = source.read(filepath).collect()

    # Test
    assert df.height == df_slv.count()
    assert df.columns == df_slv.columns

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)


def test_file_data_sink_polars_delta():

    dirpath = os.path.join(paths.tmp, "df_slv_polars_sink.delta")
    if os.path.exists(dirpath):
        shutil.rmtree(dirpath)

    # Write as overwrite
    sink = FileDataSink(
        path=dirpath,
        format="DELTA",
        mode="OVERWRITE",
    )
    sink.write(df_slv_polars)

    # Write as append
    sink.write(df_slv_polars, mode="append")

    # Read back
    source = sink.as_source()
    source.dataframe_type = "POLARS"
    df = source.read().collect()

    # Test
    assert df.height == df_slv.count() * 2
    assert df.columns == df_slv.columns

    # Cleanup
    sink.purge()
    assert not os.path.exists(sink.path)


def test_table_data_sink():

    # Write as overwrite
    sink = TableDataSink(
        catalog_name="hive_metastore",
        schema_name="default",
        table_name="slv_stock_prices_sink",
        mode="OVERWRITE",
    )
    assert sink._id == "hive_metastore.default.slv_stock_prices_sink"
    assert sink._uuid == "e2c5cef7-9288-a1e5-050f-e4660ad1176b"
    assert sink.full_name == "hive_metastore.default.slv_stock_prices_sink"
    assert sink.format == "DELTA"
    assert sink.mode == "OVERWRITE"

    # TODO: Test write using spark sessions on Databricks
    # source.write(df_slv)


if __name__ == "__main__":
    test_file_data_sink_parquet()
    test_file_data_sink_delta()
    test_file_data_sink_stream()
    test_file_data_sink_polars_parquet()
    test_file_data_sink_polars_delta()
    test_table_data_sink()
