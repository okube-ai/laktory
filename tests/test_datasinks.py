import os
import shutil
import uuid

import pytest
from pathlib import Path
import pyspark.sql.functions as F
from pyspark.errors import AnalysisException
from pyspark.errors import IllegalArgumentException

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
    source.dataframe_backend = "POLARS"
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
    source.dataframe_backend = "POLARS"
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
        schema_name="default",
        table_name="slv_stock_prices_sink",
        mode="OVERWRITE",
    )
    assert sink._id == "default.slv_stock_prices_sink"
    assert sink._uuid == "65e2c312-188a-f4e2-3d49-d9f15bec2956"
    assert sink.full_name == "default.slv_stock_prices_sink"
    assert sink.format == "DELTA"
    assert sink.mode == "OVERWRITE"

    # Write
    sink.write(df_slv)

    # Read back
    df = sink.as_source().read(spark=spark)

    # Test
    assert df.count() == df_slv.count()
    assert df.columns == df_slv.columns

    # Cleanup
    sink.purge(spark=spark)


def test_view_data_sink():

    # Create table
    table_path = Path(paths.tmp) / "hive" / f"slv_{str(uuid.uuid4())}"
    (
        df_slv.write.mode("OVERWRITE")
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
    _df = df_slv.filter(F.col("symbol") == "AAPL")
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
    test_file_data_sink_polars_parquet()
    test_file_data_sink_polars_delta()
    test_table_data_sink()
    test_view_data_sink()
