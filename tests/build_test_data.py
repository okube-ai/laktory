import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os
import yfinance as yf

from laktory import models
from datetime import datetime

# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

spark = (
    SparkSession.builder.appName("UnitTesting")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
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
                name="stock_price",
                producer={
                    "name": "yahoo-finance",
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
# Build Pandas DataFrame                                                      #
# --------------------------------------------------------------------------- #

pdf = pd.DataFrame([e.model_dump() for e in events])
# del pdf["event_root"]
# pdf.to_json(os.path.join(rootpath, "brz_stock_prices.json"))


# --------------------------------------------------------------------------- #
# Build Quickstart Data                                                       #
# --------------------------------------------------------------------------- #


with open(
    "../laktory/resources/quickstart-stacks/workflows/data/stock_prices.json", "w"
) as fp:
    for _, row in pdf.iterrows():
        del row["data"]["@id"]
        for k in list(row["data"].keys()):
            if k.startswith("_"):
                del row["data"][k]

        fp.write(row.to_json() + "\n")

# --------------------------------------------------------------------------- #
# Write Bronze DataFrame                                                      #
# --------------------------------------------------------------------------- #

df = spark.createDataFrame(
    pdf[["name", "description", "producer", "data"]],
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
df.write.parquet(os.path.join(rootpath, "brz_stock_prices"), mode="OVERWRITE")
df.write.mode("OVERWRITE").format("delta").save(
    os.path.join(rootpath, "brz_stock_prices_delta")
)


# --------------------------------------------------------------------------- #
# Build Silver DataFrame                                                      #
# --------------------------------------------------------------------------- #

# Stock prices
cols0 = df.columns
df = df.withColumn("created_at", F.col("data._created_at").cast(T.TimestampType()))
df = df.withColumn("symbol", F.col("data.symbol").cast(T.StringType()))
df = df.withColumn("open", F.col("data.open").cast(T.DoubleType()))
df = df.withColumn("close", F.col("data.close").cast(T.DoubleType()))
df = df.drop(*cols0)
df = df.repartition(1)
df.write.parquet(os.path.join(rootpath, "slv_stock_prices"), mode="OVERWRITE")
df.write.mode("OVERWRITE").format("delta").save(
    os.path.join(rootpath, "slv_stock_prices_delta")
)


# Metadata
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
df_meta = df_meta.repartition(1)
df_meta.write.format("parquet").save(
    os.path.join(rootpath, "slv_stock_meta"), mode="OVERWRITE"
)
