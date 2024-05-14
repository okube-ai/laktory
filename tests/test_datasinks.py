import os
import shutil
import pytest
from pyspark.errors import AnalysisException

from laktory.models import TableDataSink
from laktory.models import FileDataSink
from laktory._testing import df_slv
from laktory._testing import Paths
from laktory._testing import spark

paths = Paths(__file__)


def test_file_data_sink():

    dirpath = os.path.join(paths.tmp, "df_slv_sink")
    if os.path.exists(dirpath):
        shutil.rmtree(dirpath)

    # Write as overwrite
    source = FileDataSink(
        path=dirpath,
        format="PARQUET",
        mode="OVERWRITE",
    )
    source.write(df_slv)

    # Write as append
    source.write(df_slv, mode="append")

    # Write and raise error
    with pytest.raises(AnalysisException):
        source.write(df_slv, mode="error")

    # Read back
    df = spark.read.format("PARQUET").load(dirpath)

    # Test
    assert df.count() == 2 * df_slv.count()

    # Cleanup
    if os.path.exists(dirpath):
        shutil.rmtree(dirpath)


def test_table_data_sink():

    # Write as overwrite
    source = TableDataSink(
        catalog_name="hive_metastore",
        schema_name="default",
        table_name="slv_stock_prices_sink",
        mode="OVERWRITE",
    )

    assert source.full_name == "hive_metastore.default.slv_stock_prices_sink"
    assert not source.as_stream
    assert source.format == "DELTA"
    assert source.mode == "OVERWRITE"

    # TODO: Test write using spark sessions on Databricks
    # source.write(df_slv)


if __name__ == "__main__":
    test_file_data_sink()
    test_table_data_sink()
