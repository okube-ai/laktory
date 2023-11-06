from pydantic import ValidationError
from pyspark.sql import types as T
import pandas as pd
import pytest

from laktory.models import Table
from laktory._testing import table_brz
from laktory._testing import table_slv
from laktory._testing import EventsManager

manager = EventsManager()
manager.build_events()


def test_model():
    print(table_slv.model_dump())
    assert table_slv.model_dump() == {
        "name": "slv_stock_prices",
        "columns": [
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "created_at",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": [
                    {"value": "_created_at", "to_column": True, "to_lit": False},
                    {"value": "data._created_at", "to_column": True, "to_lit": False},
                ],
                "spark_func_kwargs": {},
                "spark_func_name": "coalesce",
                "sql_expression": None,
                "table_name": "slv_stock_prices",
                "type": "timestamp",
                "unit": None,
            },
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "symbol",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": [
                    {"value": "data.symbol", "to_column": True, "to_lit": False}
                ],
                "spark_func_kwargs": {},
                "spark_func_name": "coalesce",
                "sql_expression": None,
                "table_name": "slv_stock_prices",
                "type": "string",
                "unit": None,
            },
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "open",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": [
                    {"value": "data.open", "to_column": True, "to_lit": False}
                ],
                "spark_func_kwargs": {},
                "spark_func_name": "coalesce",
                "sql_expression": None,
                "table_name": "slv_stock_prices",
                "type": "double",
                "unit": None,
            },
            {
                "catalog_name": "dev",
                "comment": None,
                "name": "close",
                "pii": None,
                "schema_name": "markets",
                "spark_func_args": [],
                "spark_func_kwargs": {},
                "spark_func_name": None,
                "sql_expression": "data.open",
                "table_name": "slv_stock_prices",
                "type": "double",
                "unit": None,
            },
        ],
        "primary_key": None,
        "comment": None,
        "catalog_name": "dev",
        "schema_name": "markets",
        "grants": None,
        "data": [[None, "AAPL", 1, 2], [None, "AAPL", 3, 4], [None, "AAPL", 5, 6]],
        "timestamp_key": None,
        "event_source": None,
        "table_source": {
            "read_as_stream": True,
            "name": "brz_stock_prices",
            "schema_name": None,
            "catalog_name": None,
            "from_pipeline": True,
        },
        "zone": "SILVER",
        "pipeline_name": None,
    }

    # Invalid zone
    with pytest.raises(ValidationError):
        Table(name="googl", zone="ROUGE")


def test_data():
    print(table_slv.df)
    assert table_slv.df.equals(
        pd.DataFrame(
            {
                "created_at": [None, None, None],
                "symbol": ["AAPL", "AAPL", "AAPL"],
                "open": [1, 3, 5],
                "close": [2, 4, 6],
            }
        )
    )


def test_bronze():
    df0 = manager.to_spark_df()
    df1 = table_brz.process_bronze(df0)
    assert "_bronze_at" in df1.columns


def test_silver():
    df0 = manager.to_spark_df()
    df0.show()
    df1 = table_brz.process_bronze(df0)
    df1.show()
    df2 = table_slv.process_silver(df1)
    df2.show()
    assert df2.schema == T.StructType(
        [
            T.StructField("created_at", T.TimestampType(), True),
            T.StructField("symbol", T.StringType(), True),
            T.StructField("open", T.DoubleType(), True),
            T.StructField("close", T.DoubleType(), True),
            T.StructField("_bronze_at", T.TimestampType(), False),
            T.StructField("_silver_at", T.TimestampType(), False),
        ]
    )
    s = df2.toPandas().iloc[0]
    print(s)
    assert s["created_at"] == pd.Timestamp("2023-09-01 00:00:00")
    assert s["symbol"] == "AAPL"
    assert s["open"] == 189.49000549316406
    assert s["close"] == 189.49000549316406


if __name__ == "__main__":
    test_model()
    test_data()
    test_bronze()
    test_silver()
