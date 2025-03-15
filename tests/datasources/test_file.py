import polars as pl
import pytest

# from laktory._testing import Paths
from laktory._testing import assert_dfs_equal
from laktory._testing import sparkf
from laktory.models.datasources import FileDataSource
from laktory.readers.polarsreader import SUPPORTED_FORMATS as PL_SUPPORTED_FORMATS

# paths = Paths(__file__, "../")
spark = sparkf.spark

pl_read_tests = [("POLARS", fmt) for fmt in PL_SUPPORTED_FORMATS]


@pytest.fixture
def df0():
    return pl.DataFrame(
        {
            "a": ["a", "b", "c"],
            "y": [3, 4, 5],
        }
    )


@pytest.mark.parametrize(
    ["backend", "fmt"],
    # ["backend", "path"],
    pl_read_tests,
    # [
    #     # ("PYSPARK", paths.data / "events/yahoo-finance/stock_price"),  # JSON
    #     # ("POLARS", paths.data / "events/yahoo-finance/stock_price"),  # JSON
    #     # ("PYSPARK", "json"),  # JSON
    #     ("POLARS", "json"),  # JSON
    #     # ("PYSPARK", data_list),
    #     # ("POLARS", data_list),
    # ],
)
def test_read(backend, fmt, df0, tmp_path):
    filepath = tmp_path / f"df.{fmt}"

    if fmt == "AVRO":
        df0.write_avro(filepath)
    elif fmt == "CSV":
        df0.write_csv(filepath)
    elif fmt == "EXCEL":
        pytest.skip("Missing library. Skipping Test.")
    elif fmt == "DELTA":
        filepath = tmp_path
        df0.write_delta(filepath)
    elif fmt == "JSON":
        df0.write_json(filepath)
    elif fmt in ["JSONL", "NDJSON"]:
        df0.write_ndjson(filepath)
    elif fmt == "IPC":
        df0.write_ipc(filepath)
    elif fmt == "PARQUET":
        df0.write_parquet(filepath)
    elif fmt == "PYARROW":
        import pyarrow.dataset as ds

        filepath = tmp_path
        ds.write_dataset(
            data=df0.to_arrow(), base_dir=filepath, format="parquet", partitioning=None
        )
    else:
        raise NotImplementedError()

    source = FileDataSource(
        path=filepath,
        format=fmt,
        dataframe_backend=backend,
    )
    df = source.read(spark=spark)
    assert_dfs_equal(df, df0)
