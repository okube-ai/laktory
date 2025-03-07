import pandas as pd

from laktory._testing import Paths
from laktory._testing import sparkf
from laktory.models import DataFrameSchema
from laktory.models import dtypes
from laktory.models.datasources import FileDataSource
from laktory.models.datasources import MemoryDataSource
from laktory.models.datasources import TableDataSource

paths = Paths(__file__)
spark = sparkf.spark


schema = DataFrameSchema(
    columns={
        "data": dtypes.Struct(
            fields={
                "@id": dtypes.String(),
                "_created_at": dtypes.String(),
                "_name": dtypes.String(),
                "_producer_name": dtypes.String(),
                "close": dtypes.Float64(),
                "created_at": dtypes.String(),
                "high": dtypes.Float64(),
                "low": dtypes.Float64(),
                "open": dtypes.Float64(),
                "symbol": dtypes.String(),
            }
        ),
        "description": dtypes.String(),
        "name": dtypes.String(),
        "producer": dtypes.Struct(
            fields={
                "description": dtypes.String(),
                "name": dtypes.String(),
                "party": dtypes.Int64(),
            }
        ),
    },
)

# DataFrame
pdf = pd.DataFrame(
    {
        "x": [1, 2, 3],
        "a": [1, -1, 1],
        "b": [2, 0, 2],
        "c": [3, 0, 3],
        "n": [4, 0, 4],
    },
)
df0 = spark.createDataFrame(pdf)


def test_file_data_source():
    source = FileDataSource(
        path="Volumes/sources/landing/events/yahoo_finance/stock_price"
    )

    assert source.path == "Volumes/sources/landing/events/yahoo_finance/stock_price"
    assert source.df_backend == "SPARK"
    assert not source.as_stream


def test_file_data_source_read():
    source = FileDataSource(
        path=paths.data / "events/yahoo-finance/stock_price",
        as_stream=False,
    )
    df = source.read(spark)
    assert df.to_native().count() == 80
    assert df.columns == [
        "data",
        "description",
        "name",
        "producer",
    ]


def test_file_data_source_read_jsonl():
    source = FileDataSource(
        path=paths.data / "events/yahoo-finance/stock_price",
        format="JSONL",
    )
    df = source.read(spark)
    assert df.to_native().count() == 80
    assert df.columns == [
        "data",
        "description",
        "name",
        "producer",
    ]

    return df


def test_file_data_source_read_schema():
    source = FileDataSource(
        path=paths.data / "events/yahoo-finance/stock_price",
        as_stream=False,
        schema=schema,
    )

    # Dump and read source to ensure schema alias is properly handled
    source = FileDataSource(**source.model_dump(exclude_unset=True))

    df = source.read(spark)
    assert df.to_native().count() == 80
    assert df.columns == [
        "data",
        "description",
        "name",
        "producer",
    ]
    assert df.schema == schema.to_narwhals()

    # # Schema as string
    # # TODO: Add support?
    # source = FileDataSource(
    #     path=paths.data / "events/yahoo-finance/stock_price",
    #     as_stream=False,
    #     schema="data STRUCT<_created_at STRING, _name STRING, _producer_name STRING, close DOUBLE, created_at STRING, high DOUBLE, low DOUBLE, open DOUBLE, symbol STRING>, description STRING, name STRING, producer STRUCT<description STRING, name STRING, party LONG>",
    # )
    # df = source.read(spark)
    # assert df.count() == 80
    # assert df.columns == [
    #     "data",
    #     "description",
    #     "name",
    #     "producer",
    # ]


def test_file_data_source_polars():
    source = FileDataSource(
        path=paths.data
        / "brz_stock_prices/part-00000-877096dd-1964-482e-9873-76361150a331-c000.snappy.parquet",
        format="PARQUET",
        dataframe_backend="POLARS",
        filter="data.open > 300",
        selects={
            "data.created_at": "created_at",
            "data.symbol": "symbol",
            "data.open": "open",
            "data.close": "close",
            "data.high": "high2",
            "data.low": "low2",
            "data._created_at": "_created_at",
        },
        drops=[
            "_created_at",
        ],
        renames={
            "low2": "low",
            "high2": "high",
        },
        # sample={
        #     "fraction": 0.5,
        # },
    )
    df = source.read().to_native().collect()

    assert df["open"].min() > 300
    assert df.columns == ["created_at", "symbol", "open", "close", "high", "low"]
    assert df.height == 20


def test_memory_data_source(df0=df0):
    source = MemoryDataSource(
        df=df0,
        filter="b != 0",
        selects=["a", "b", "c"],
        renames={"a": "aa", "b": "bb", "c": "cc"},
        broadcast=True,
    )

    # Test reader
    df = source.read(spark)
    assert df.columns == ["aa", "bb", "cc"]
    assert df.count() == 2
    # assert df.toPandas()["chain"].tolist() == ["chain", "chain"]

    # Select with rename
    source = MemoryDataSource(
        df=df0,
        selects={"x": "x1", "n": "n1"},
    )
    df = source.read(spark)
    assert df.columns == ["x1", "n1"]
    assert df.count() == 3

    # Drop
    source = MemoryDataSource(
        df=df0,
        drops=["b", "c", "n"],
    )
    df = source.read(spark)
    df.show()
    assert df.columns == ["x", "a"]
    assert df.count() == 3


def test_drop_duplicates(df0=df0):
    df1 = df0.union(df0)
    assert df1.count() == df0.count() * 2

    # Drop All
    df = MemoryDataSource(df=df1, drop_duplicates=True).read(spark)
    assert df.count() == df0.count()

    # Drop columns a and n
    df = MemoryDataSource(df=df1, drop_duplicates=["a", "n"]).read(spark)
    assert df.count() == 2


def test_memory_data_source_from_dict():
    data0 = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 3, "y": "c"}]
    data1 = {"x": [1, 2, 3], "y": ["a", "b", "c"]}
    df_ref = pd.DataFrame(data0)

    # Spark - data0
    source = MemoryDataSource(data=data0, dataframe_backend="SPARK")
    df = source.read(spark)
    assert df.toPandas().equals(df_ref)

    # Spark - data1
    source = MemoryDataSource(data=data1, dataframe_backend="SPARK")
    df = source.read(spark)
    assert df.toPandas().equals(df_ref)

    # Polars - data0
    source = MemoryDataSource(data=data0, dataframe_backend="POLARS")
    df = source.read(spark)
    assert df.collect().to_pandas().equals(df_ref)

    # Polars - data1
    source = MemoryDataSource(data=data1, dataframe_backend="POLARS")
    df = source.read(spark)
    assert df.collect().to_pandas().equals(df_ref)


def test_table_data_source():
    source = TableDataSource(
        catalog_name="dev",
        schema_name="finance",
        table_name="slv_stock_prices",
    )

    # Test meta
    assert source.full_name == "dev.finance.slv_stock_prices"
    assert source._id == "dev.finance.slv_stock_prices"


if __name__ == "__main__":
    test_file_data_source()
    test_file_data_source_read()
    test_file_data_source_read_jsonl()
    test_file_data_source_read_schema()
    test_file_data_source_polars()
    # test_memory_data_source()
    # test_drop_duplicates()
    # test_memory_data_source_from_dict()
    # test_table_data_source()
