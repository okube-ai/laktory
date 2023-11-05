from laktory.models import DataEventHeader
from laktory.models import DataEvent
from laktory.models import Producer
from laktory.models import Table
from laktory.models import TableDataSource
from laktory.models import EventDataSource
from laktory.models import Pipeline
from datetime import datetime


# --------------------------------------------------------------------------- #
# Events                                                                      #
# --------------------------------------------------------------------------- #

class StockPriceDataEventHeader(DataEventHeader):
    name: str = "stock_price"
    producer: Producer = Producer(name="yahoo-finance")


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
                    self.events[-1].events_root = events_root

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
        from pyspark.sql import SparkSession
        import pyspark.sql.types as T

        spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
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
    zone="BRONZE",
    catalog_name="dev",
    schema_name="markets",
    event_source=EventDataSource(
        name="stock_price",
    ),
)

table_slv = Table(
    name="slv_stock_prices",
    columns=[
        {
            "name": "created_at",
            "type": "timestamp",
            "spark_func_name": "coalesce",
            "spark_func_args": ["_created_at", "data._created_at"],
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
    data=[[1, 2], [3, 4], [5, 6]],
    zone="SILVER",
    catalog_name="dev",
    schema_name="markets",
    table_source=TableDataSource(
        name="brz_stock_prices",
    ),
)


# --------------------------------------------------------------------------- #
# Pipeline                                                                    #
# --------------------------------------------------------------------------- #

class StockPricesPipeline(Pipeline):
    name: str = "pl-stock-prices"
    tables: list[Table] = [
        table_brz,
        table_slv,
    ]


if __name__ == "__main__":
    manager = EventsManager()
    manager.build_events(events_root="./events/")
    manager.to_path()
    # manager.to_azure_storage()
