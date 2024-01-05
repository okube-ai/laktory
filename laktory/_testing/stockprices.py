import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.types as T

from laktory.models import DataEventHeader
from laktory.models import DataEvent
from laktory.models import DataProducer
from laktory.models import Table
from laktory.models import Pipeline
from laktory.models.databricks.pipeline import PipelineUDF
from datetime import datetime

spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


# --------------------------------------------------------------------------- #
# Events                                                                      #
# --------------------------------------------------------------------------- #


class StockPriceDataEventHeader(DataEventHeader):
    name: str = "stock_price"
    producer: DataProducer = DataProducer(name="yahoo-finance")


class StockPriceDataEvent(StockPriceDataEventHeader, DataEvent):
    pass


class EventsManager:
    def __init__(self):
        self.events = []

    def build_events(self, events_root: str = None):
        import yfinance as yf

        symbols = [
            "AAPL",
            "AMZN",
            "GOOGL",
            "MSFT",
        ]

        t0 = datetime(2023, 9, 1)
        t1 = datetime(2023, 9, 30)

        self.events = []
        for s in symbols:
            df = yf.download(s, t0, t1, interval="1d")
            for _, row in df.iterrows():
                self.events += [
                    StockPriceDataEvent(
                        data={
                            "created_at": _,
                            "symbol": s,
                            "open": float(
                                row["Open"]
                            ),  # np.float64 are not supported for serialization
                            "close": float(row["Close"]),
                            "high": float(row["High"]),
                            "low": float(row["Low"]),
                            "@id": "_id",
                        },
                    )
                ]
                if events_root:
                    self.events[-1].events_root_ = events_root

        return self.events

    def to_path(self):
        for event in self.events:
            event.to_path(suffix=event.data["symbol"], skip_if_exists=True)

    def to_azure_storage(self):
        for event in self.events:
            event.to_azure_storage_container(skip_if_exists=True)

    def to_pandas_df(self):
        import pandas as pd

        return pd.DataFrame([e.model_dump() for e in self.events])

    def to_spark_df(self):
        return spark.createDataFrame(
            [e.model_dump() for e in self.events],
            schema=T.StructType(
                [
                    T.StructField("name", T.StringType()),
                    T.StructField("description", T.StringType()),
                    T.StructField(
                        "producer",
                        T.StructType(
                            [
                                T.StructField("name", T.StringType()),
                                T.StructField("description", T.StringType()),
                                T.StructField("party", T.IntegerType()),
                            ]
                        ),
                    ),
                    T.StructField(
                        "data",
                        T.StructType(
                            [
                                T.StructField("created_at", T.StringType()),
                                T.StructField("symbol", T.StringType()),
                                T.StructField("open", T.DoubleType()),
                                T.StructField("close", T.DoubleType()),
                                T.StructField("high", T.DoubleType()),
                                T.StructField("low", T.DoubleType()),
                                T.StructField("_name", T.StringType()),
                                T.StructField("_producer_name", T.StringType()),
                                T.StructField("_created_at", T.StringType()),
                                T.StructField("@id", T.StringType()),
                            ]
                        ),
                    ),
                ]
            ),
        )


# --------------------------------------------------------------------------- #
# Tables                                                                      #
# --------------------------------------------------------------------------- #


table_brz = Table(
    name="brz_stock_prices",
    catalog_name="dev",
    schema_name="markets",
    builder={
        "event_source": {
            "name": "stock_price",
        },
        "layer": "BRONZE",
    },
)

table_slv = Table(
    name="slv_stock_prices",
    columns=[
        {
            "name": "created_at",
            "type": "timestamp",
            "spark_func_name": "coalesce",
            "spark_func_args": ["data._created_at"],
        },
        {
            "name": "symbol",
            "type": "string",
            "spark_func_name": "coalesce",
            "spark_func_args": ["data.symbol"],
        },
        {
            "name": "open",
            "type": "double",
            "spark_func_name": "coalesce",
            "spark_func_args": ["data.open"],
        },
        {"name": "close", "type": "double", "sql_expression": "data.open"},
    ],
    data=[
        ["2023-11-01T00:00:00Z", "AAPL", 1, 2],
        ["2023-11-01T01:00:00Z", "AAPL", 3, 4],
        ["2023-11-01T00:00:00Z", "GOOGL", 3, 4],
        ["2023-11-01T01:00:00Z", "GOOGL", 5, 6],
    ],
    catalog_name="dev",
    schema_name="markets",
    builder={
        "table_source": {
            "name": "brz_stock_prices",
        },
        "layer": "SILVER",
    },
    expectations=[
        {"name": "positive_price", "expression": "open > 0", "action": "FAIL"},
        {
            "name": "recent_price",
            "expression": "created_at > '2023-01-01'",
            "action": "DROP",
        },
    ],
)


df_meta = spark.createDataFrame(
    pd.DataFrame(
        {
            "symbol2": ["AAPL", "GOOGL", "AMZN"],
            "currency": ["USD"] * 3,
            "first_traded": [
                "1980-12-12T14:30:00.000Z",
                "2004-08-19T13:30:00.00Z",
                "1997-05-15T13:30:00.000Z",
            ],
        }
    )
)

df_name = spark.createDataFrame(
    pd.DataFrame(
        {"symbol3": ["AAPL", "GOOGL", "AMZN"], "name": ["Apple", "Google", "Amazon"]}
    )
)

table_slv_star = Table(
    name="slv_star_stock_prices",
    catalog_name="dev",
    schema_name="markets",
    builder={
        "layer": "SILVER_STAR",
        "table_source": {
            "name": "slv_stock_prices",
            "filter": "created_at = '2023-11-01T00:00:00Z'",
        },
        "joins": [
            {
                "other": {
                    "name": "slv_stock_metadata",
                    "selects": {
                        "symbol2": "symbol",
                        "currency": "currency",
                        "first_traded": "last_traded",
                    },
                },
                "on": ["symbol"],
            },
            {
                "other": {
                    "name": "slv_stock_names",
                    "selects": [
                        "symbol3",
                        "name",
                    ],
                },
                "on": ["symbol3"],
            },
        ],
    },
    columns=[
        {
            "name": "symbol3",
            "spark_func_name": "coalesce",
            "spark_func_args": ["symbol"],
        }
    ],
)
table_slv_star.builder.source._df = table_slv.to_df(spark=spark)
table_slv_star.builder.joins[0].other._df = df_meta
table_slv_star.builder.joins[1].other._df = df_name

table_gld = Table(
    name="gld_stock_prices",
    columns=[
        {
            "name": "name2",
            "type": "string",
            "spark_func_name": "coalesce",
            "spark_func_args": [
                "name",
            ],
        }
    ],
    catalog_name="dev",
    schema_name="markets",
    builder={
        "table_source": {
            "name": "slv_stock_prices",
        },
        "layer": "GOLD",
        "drop_columns": ["max_close"],
        "template": "GOLD1",
        "aggregation": {
            "groupby_columns": ["symbol"],
            "agg_expressions": [
                {
                    "name": "min_open",
                    "spark_func_name": "min",
                    "spark_func_args": ["open"],
                },
                {
                    "name": "max_open",
                    "sql_expression": "max(open)",
                },
                {
                    "name": "min_close",
                    "sql_expression": "max(open)",
                },
                {
                    "name": "max_close",
                    "sql_expression": "max(open)",
                },
            ],
        },
        "joins_post_aggregation": [
            {
                "other": {
                    "name": "slv_stock_names",
                    "selects": {
                        "symbol3": "symbol",
                        "name": "name",
                    },
                },
                "on": ["symbol"],
            }
        ],
    },
)
table_gld.builder.joins_post_aggregation[0].other._df = df_name

# --------------------------------------------------------------------------- #
# Views                                                                       #
# --------------------------------------------------------------------------- #

view = None
# view = View(
#     name="slv_googl_prices",
#     catalog_name="dev",
#     schema_name="markets",
#     sql_expression="SELECT * FROM dev.markets.slv_star_stock_prices"
# )

# --------------------------------------------------------------------------- #
# Pipeline                                                                    #
# --------------------------------------------------------------------------- #


class StockPricesPipeline(Pipeline):
    name: str = "pl-stock-prices"
    tables: list[Table] = [
        table_brz,
        table_slv,
    ]
    udfs: list[PipelineUDF] = [
        {
            "module_name": "stock_functions",
            "function_name": "high",
        }
    ]


if __name__ == "__main__":
    manager = EventsManager()
    manager.build_events(events_root="./events/")
    manager.to_path()
    # manager.to_azure_storage()
