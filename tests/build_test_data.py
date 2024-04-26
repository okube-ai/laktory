import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import os
import yfinance as yf

from laktory import models
from datetime import datetime

# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

symbols = [
    "AAPL",
    "AMZN",
    "GOOGL",
    "MSFT",
]

rootpath = os.path.join(os.path.dirname(__file__), "data/")

t0 = datetime(2023, 9, 1)
t1 = datetime(2023, 9, 30)


# --------------------------------------------------------------------------- #
# Build Events                                                                #
# --------------------------------------------------------------------------- #


events = []
for s in symbols:
    df = yf.download(s, t0, t1, interval="1d")
    for _, row in df.iterrows():
        events += [
            models.DataEvent(
                name="stock_prices",
                producer={
                    "name": "yahooo-finance",
                },
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
        events[-1].events_root_ = os.path.join(rootpath, "events/")


# --------------------------------------------------------------------------- #
# Write Events                                                                #
# --------------------------------------------------------------------------- #

for event in events:
    event.to_path(suffix=event.data["symbol"], skip_if_exists=True)


# --------------------------------------------------------------------------- #
# Write Pandas DataFrame                                                      #
# --------------------------------------------------------------------------- #

pdf = pd.DataFrame([e.model_dump() for e in events])
pdf.to_json(os.path.join(rootpath, "events_raw_df.json"))


# --------------------------------------------------------------------------- #
# Write Spark DataFrame                                                       #
# --------------------------------------------------------------------------- #

df = spark.createDataFrame(
    pdf,
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
df = df.repartition(1)
df.write.parquet(os.path.join(rootpath, "events_raw_spark"))
