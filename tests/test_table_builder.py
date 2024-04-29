# import pytest
# from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# import pandas as pd
# import datetime

from laktory import models

# from laktory._testing import EventsManager
# from laktory._testing import df_meta
# from laktory._testing import table_brz
# from laktory._testing import table_slv
# from laktory._testing import table_slv_star
# from laktory._testing import table_gld
# from laktory.models import Table
# from laktory.models import TableJoin
# from laktory.models import TableUnion
# from laktory.models import TableWindowFilter
# from laktory.models import TableAggregation

# Spark
spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

df_brz = spark.read.parquet("./data/brz_stock_prices")
df_slv = spark.read.parquet("./data/slv_stock_prices")


def test_read_and_process():
    builder = models.TableBuilder(
        event_source={
            "name": "brz_stock_prices",
            "mock_df": df_brz,
        },
        spark_chain={
            "nodes": [
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "sql_expression": "data._created_at",
                },
                {"name": "symbol", "type": "string", "sql_expression": "data.symbol"},
                {"name": "open", "type": "double", "sql_expression": "data.open"},
                {"name": "close", "type": "double", "sql_expression": "data.close"},
                {
                    "spark_func_name": "drop",
                    "spark_func_args": [
                        "description",
                        "data",
                        "producer",
                    ],
                },
            ]
        },
    )

    # Read and process
    df = builder.read_source(spark)
    df = builder.process(df)

    # Test
    assert df.columns == ["name", "created_at", "symbol", "open", "close"]
    assert df.count() == 80


def test_bronze():
    builder = models.TableBuilder(
        layer="BRONZE",
        event_source={
            "name": "brz_stock_prices",
            "mock_df": df_brz,
        },
        spark_chain={
            "nodes": [
                {"name": "symbol", "type": "string", "sql_expression": "data.symbol"},
            ]
        },
    )

    # Read and process
    df = builder.read_source(spark)
    df = builder.process(df)

    # Test
    assert not builder.drop_source_columns
    assert not builder.drop_duplicates
    assert builder.layer == "BRONZE"
    assert builder.template == "BRONZE"
    assert builder.add_laktory_columns
    assert df.columns == [
        "name",
        "description",
        "producer",
        "data",
        "symbol",
        "_bronze_at",
    ]
    assert df.count() == 80


def test_silver():
    df = df_brz.select(df_brz.columns)
    df = df.withColumn("_bronze_at", F.current_timestamp())
    df = df.union(df)

    builder = models.TableBuilder(
        layer="SILVER",
        table_source={
            "name": "slv_stock_prices",
            "mock_df": df,
        },
        spark_chain={
            "nodes": [
                {"name": "symbol", "type": "string", "sql_expression": "data.symbol"},
            ]
        },
        drop_duplicates=True,
    )

    # Read and process
    df = builder.read_source(spark)
    df = builder.process(df)

    # Test
    assert builder.drop_source_columns
    assert builder.drop_duplicates
    assert builder.layer == "SILVER"
    assert builder.template == "SILVER"
    assert builder.add_laktory_columns
    assert df.columns == ["_bronze_at", "symbol", "_silver_at"]
    assert df.count() == 80

#
# def test_table_agg():
#     agg = TableAggregation(
#         groupby_columns=["symbol"],
#         agg_expressions=[
#             {"name": "min_open", "spark_func_name": "min", "spark_func_args": ["open"]},
#             {
#                 "name": "max_open",
#                 "sql_expression": "max(open)",
#             },
#         ],
#     )
#     df = table_slv.to_df(spark)
#     df1 = agg.execute(df=df)
#     df1.show()
#     data = df1.toPandas().to_dict(orient="records")
#     print(data)
#     assert data == [
#         {"symbol": "AAPL", "min_open": 1, "max_open": 3},
#         {"symbol": "GOOGL", "min_open": 3, "max_open": 5},
#     ]
#
#
# def test_table_agg_window():
#     agg = TableAggregation(
#         groupby_window={
#             "time_column": "created_at",
#             "window_duration": "1 day",
#             # "start_time": "2000-01-01T00:00:00Z"
#         },
#         groupby_columns=["symbol"],
#         agg_expressions=[
#             {"name": "min_open", "spark_func_name": "min", "spark_func_args": ["open"]},
#             {
#                 "name": "max_open",
#                 "sql_expression": "max(open)",
#             },
#         ],
#     )
#     df = table_slv.to_df(spark)
#     assert df.count() == 4
#
#     df1 = agg.execute(df=df)
#     df1.select("window.start", "window.end").show()
#     df1.printSchema()
#
#     # Convert window to string for easier testing
#     df1 = df1.withColumn("start", F.col("window.start").cast("string"))
#     df1 = df1.withColumn("end", F.col("window.end").cast("string"))
#     df1 = df1.drop("window")
#     df1.printSchema()
#
#     data = [row.asDict(recursive=True) for row in df1.collect()]
#     print(data)
#     assert data == [
#         {
#             "symbol": "AAPL",
#             "min_open": 1,
#             "max_open": 3,
#             "start": "2023-11-01 00:00:00",
#             "end": "2023-11-02 00:00:00",
#         },
#         {
#             "symbol": "GOOGL",
#             "min_open": 3,
#             "max_open": 5,
#             "start": "2023-11-01 00:00:00",
#             "end": "2023-11-02 00:00:00",
#         },
#     ]
#
#
# def test_table_window_filter():
#     w = TableWindowFilter(
#         partition_by=["symbol"],
#         order_by=[
#             {"sql_expression": "created_at", "desc": True},
#         ],
#         drop_row_index=False,
#         rows_to_keep=1,
#     )
#     df = table_slv.to_df(spark)
#     df.show()
#     df1 = w.execute(df=df)
#     df1.show()
#     data = df1.toPandas().to_dict(orient="records")
#     print(data)
#     assert data == [
#         {
#             "created_at": "2023-11-01T01:00:00Z",
#             "symbol": "AAPL",
#             "open": 3,
#             "close": 4,
#             "_row_index": 1,
#         },
#         {
#             "created_at": "2023-11-01T01:00:00Z",
#             "symbol": "GOOGL",
#             "open": 5,
#             "close": 6,
#             "_row_index": 1,
#         },
#     ]
#
#
#
#
# def test_silver_star():
#     df = table_slv_star.builder.read_source(spark)
#     df = table_slv_star.builder.process(df, spark=spark)
#     df.printSchema()
#
#     assert "_silver_star_at" in df.columns
#     data = df.toPandas().drop("_silver_star_at", axis=1).to_dict("records")
#     print(data)
#     assert data == [
#         {
#             "created_at": "2023-11-01T00:00:00Z",
#             "symbol": "AAPL",
#             "open": 1,
#             "close": 2,
#             "symbol3": "AAPL",
#             "currency": "USD",
#             "last_traded": "1980-12-12T14:30:00.000Z",
#             "name": "Apple",
#         },
#         {
#             "created_at": "2023-11-01T00:00:00Z",
#             "symbol": "GOOGL",
#             "open": 3,
#             "close": 4,
#             "symbol3": "GOOGL",
#             "currency": "USD",
#             "last_traded": "2004-08-19T13:30:00.00Z",
#             "name": "Google",
#         },
#     ]
#
#
#
# def test_builder_agg():
#     # Get Data
#     df0 = manager.to_spark_df()
#     df1 = table_brz.builder.process(df0)
#     df2 = table_slv.builder.process(df1)
#     df2.show()
#
#     # Set table that needs to aggregate on input column and create new column
#     # on aggregated result
#     table_agg = Table(
#         name="table_agg",
#         catalog_name="dev",
#         schema_name="markets",
#         columns=[
#             {
#                 "name": "close_mean_2",
#                 "spark_func_name": "scaled_power",
#                 "spark_func_args": [
#                     "close_mean",
#                     {
#                         "value": 2,
#                         "to_lit": True,
#                     },
#                     {
#                         "value": 1,
#                         "to_lit": True,
#                     },
#                 ],
#             }
#         ],
#         builder={
#             "aggregation": {
#                 "groupby_columns": ["symbol"],
#                 "agg_expressions": [
#                     {
#                         "name": "close_mean",
#                         "spark_func_name": "mean",
#                         "spark_func_args": ["close"],
#                     },
#                 ],
#             }
#         },
#     )
#
#     # Process Data
#     df3 = table_agg.builder.process(df2)
#     df3.show()
#
#     # Test
#     pdf = df3.toPandas()
#     s0 = pdf["close_mean"].tolist()
#     s1 = (0.5 * pdf["close_mean_2"].astype(float)).tolist()
#
#     assert s1 == pytest.approx(s0)
#
#
# def test_builder_union():
#     # Set table using union
#     df0 = manager.to_spark_df()
#     df1 = table_brz.builder.process(df0)
#
#     # Table union
#     table_union = Table(
#         name="table_union",
#         catalog_name="dev",
#         schema_name="markets",
#         builder={
#             "layer": "SILVER",
#             "drop_source_columns": False,
#             "table_source": {
#                 "name": "slv_stock_prices",
#             },
#             "unions": [{"other": {"name": "brz_stock_prices"}}],
#         },
#     )
#     table_union.builder.unions[0].other._df = df1
#
#     df1.show()
#
#     df2 = table_union.builder.process(df1)
#     df2.show()
#
#     assert df2.count() == df1.count() * 2
#
#
# def test_cdc():
#     table = Table(
#         name="brz_users_type1",
#         builder={
#             "table_source": {
#                 "name": "brz_users_cdc",
#                 "cdc": {
#                     "primary_keys": ["userId"],
#                     "sequence_by": "sequenceNum",
#                     "apply_as_deletes": "operation = 'DELETE'",
#                     "scd_type": 1,
#                     "except_columns": ["operation", "sequenceNum"],
#                 },
#             },
#         },
#     )
#
#     assert table.builder.apply_changes_kwargs == {
#         "apply_as_deletes": "operation = 'DELETE'",
#         "apply_as_truncates": None,
#         "column_list": [],
#         "except_column_list": ["operation", "sequenceNum"],
#         "ignore_null_updates": None,
#         "keys": ["userId"],
#         "sequence_by": "sequenceNum",
#         "source": "brz_users_cdc",
#         "stored_as_scd_type": 1,
#         "target": "brz_users_type1",
#         "track_history_column_list": None,
#         "track_history_except_column_list": None,
#     }
#     assert table.builder.is_from_cdc
#
#     # TODO: Run test with demo data
#     # from pyspark.sql import SparkSession
#
#     # spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
#     #
#     # df_cdc = spark.createDataFrame(pd.DataFrame({
#     #     "userId": [124, 123, 125, 126, 123, 125, 125, 123],
#     #     "name": ["Raul", "Isabel", "Mercedes", "Lily", None, "Mercedes", "Mercedes", "Isabel"],
#     #     "city": ["Oaxaca", "Monterrey", "Tijuana", "Cancun", None, "Guadalajara", "Mexicali", "Chihuahua"],
#     #     "operation": ["INSERT", "INSERT", "INSERT", "INSERT", "DELETE", "UPDATE", "UPDATE", "UPDATE"],
#     #     "sequenceNum": [1, 1, 2, 2, 6, 6, 5, 5],
#     # }))
#     # df_cdc.show()
#


if __name__ == "__main__":
    test_read_and_process()
    test_bronze()
    test_silver()
    # test_table_join()
    # test_table_join_expression()
    # test_table_join_outer()
    # test_table_union()
    # test_table_agg()
    # test_table_agg_window()
    # test_table_window_filter()
    # test_bronze()
    # test_silver()
    # test_silver_star()
    # test_gold()
    # test_cdc()
    # test_drop_duplicates()
    # test_builder_union()
    # test_builder_agg()
#
