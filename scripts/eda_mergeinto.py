import time
import datetime
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from laktory import models

from collections import defaultdict


# --------------------------------------------------------------------------- #
# Spark                                                                       #
# --------------------------------------------------------------------------- #

spark = DatabricksSession.builder.clusterId("0709-135558-ql8jcu2x").getOrCreate()
# spark = (
#     SparkSession.builder.appName("WriteMerge")
#     .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#     .config(
#         "spark.sql.catalog.spark_catalog",
#         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#     )
#     .getOrCreate()
# )
# spark.conf.set("spark.sql.session.timeZone", "UTC")


# --------------------------------------------------------------------------- #
# Functions                                                                   #
# --------------------------------------------------------------------------- #

path = "/Volumes/q01/sources/input/tmp/stocks_target"

def build_df(nrows=500, nstocks=10, t0=datetime.datetime(2024, 8, 23)):
    ntstamps = int(nrows / nstocks)

    # Timestamps
    timestamps0 = pd.date_range(datetime.datetime(2020, 8, 23), t0, freq="min")[-ntstamps:].tolist()
    timestamps1 = pd.date_range(t0, t0 + datetime.timedelta(days=1), freq="min").tolist()
    timestamps = timestamps0 + timestamps1
    timestamps = np.unique(timestamps)

    # Symbols
    symbols = [f"S{i:03d}" for i in range(nstocks)]

    # Create a schema for the DataFrame
    schema = F.StructType([
        T.StructField("stock_symbol", T.StringType(), True),
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("open", T.DoubleType(), True),
        T.StructField("close", T.DoubleType(), True),
        T.StructField("high", T.DoubleType(), True),
        T.StructField("low", T.DoubleType(), True)
    ])

    # Create an empty DataFrame with the schema
    # df = spark.createDataFrame([], schema)

    df_symbols = spark.createDataFrame(pd.DataFrame({"symbol": symbols}))
    df_timestamps = spark.createDataFrame(pd.DataFrame({"timestamp": timestamps}))
    df = df_timestamps.crossJoin(df_symbols)

    # Add columns with random numbers
    for c in ["open", "close", "high", "low"]:
        df = df.withColumn(c, F.rand())

    df = df.withColumn("_from", F.lit("target"))
    df = df.withColumn("_is_updated", F.col("symbol").isin([f"S{i:03d}" for i in [0, 1, 2]]))
    df = df.withColumn("_is_new", F.col("timestamp") > t0)
    df = df.withColumn("_is_deleted", F.col("symbol").isin([f"S{i:03d}" for i in [nstocks-1]]))

    return df


def write_target(df):
    (
        df
        .filter(~F.col("_is_new"))
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def get_source(df):
    c1 = F.col("_is_new")
    c2 = F.col("_is_updated")
    c3 = F.col("_is_deleted")
    return df.filter(c1 | c2 | c3)


def merge(source):

    keys = ["symbol", "timestamp"]

    table_target = DeltaTable.forPath(spark, path)
    (
        table_target.alias("target")
        .merge(
            source.alias("source"),
            # how="outer",
            " AND ".join([f"source.{c} = target.{c}" for c in keys]),
        )
        .whenMatchedUpdate(
            set={f"target.{c}": f"source.{c}" for c in ["open", "close", "high", "low", "_from"]},
            condition="source._is_updated = true",
        )
        .whenNotMatchedInsert(
            # values = {f"target.{c}": f"source.{c}" for c in df.columns if not c.startswith("_")}
            values={f"target.{c}": f"source.{c}" for c in source.columns}
        )
    ).execute()



# class Stocks:
#     def __init__(self, nrows=500, nstocks=500):
#         self.nstocks = nstocks
#         self.nrows = nrows
#         self.metrics = defaultdict(lambda: [])
#
#         # Paths
#         self.path = "/Volumes/q01/sources/input/tmp/stocks_target"
#
#         # Timestamps
#         self.T0 = datetime.datetime(2024, 8, 23)
#
#         # DataFrames
#         self._df = self.build_df()
#
#     @property
#     def target(self):
#         return self._df.filter(~F.col("_is_new"))
#
#     def get_source(self, incremental=True, update_frac=0.01):
#         if incremental:
#             df = self._df.filter(F.col("_is_new"))
#         else:
#             df = self._df.select(self._df.columns)
#             df = df.withColumn("_is_updated", F.col("symbol").isin([f"S{i:03d}" for i in range(int(update_frac*self.nstocks))]))
#         df = df.withColumn("_from", F.lit("source"))
#         return df
#
#     @property
#     def keys(self):
#         return ["symbol", "timestamp"]

#
#     def append(self, incremental=True):
#         _k1 = "increment" if incremental else "dump"
#         self.write_target()
#         source = self.get_source(incremental=incremental)
#         # df = get_source(increment)
#         t0 = time.time()
#         print(f"Processing {_k1} as append...", end="")
#         source.write.format("DELTA").mode("append").save(self.path)
#         dt = print_done(t0)
#         return dt
#
#     def overwrite(self, incremental=True):
#         _k1 = "increment" if incremental else "dump"
#         if incremental:
#             self.write_target()
#         source = self.get_source(incremental=incremental)
#         t0 = time.time()
#         print(f"Processing {_k1} as overwrite...", end="")
#         if incremental:
#             df0 = spark.read.format("DELTA").load(self.path)
#             df = df0.union(source)
#         else:
#             df = source
#         df.write.format("DELTA").mode("overwrite").save(self.path)
#         dt = print_done(t0)
#         return dt
#
#     def merge(self, incremental=True):
#         _k1 = "increment" if incremental else "dump"
#         self.write_target()
#         source = self.get_source(incremental)
#         t0 = time.time()
#         print(f"Processing {_k1} as merge...", end="")
#         table_target = DeltaTable.forPath(spark, self.path)
#         (
#             table_target.alias("target")
#             .merge(
#                 source.alias("source"),
#                 # how="outer",
#                 " AND ".join([f"source.{c} = target.{c}" for c in self.keys]),
#             )
#             .whenMatchedUpdate(
#                 set={f"target.{c}": f"source.{c}" for c in ["open", "close", "high", "low", "_from"]},
#                 condition="source._is_updated = true",
#             )
#             .whenNotMatchedInsert(
#                 # values = {f"target.{c}": f"source.{c}" for c in df.columns if not c.startswith("_")}
#                 values = {f"target.{c}": f"source.{c}" for c in source.columns}
#             )
#         ).execute()
#         dt = print_done(t0)
#         return dt
#
#     def benchmark(self, sizes, nruns=3):
#         self.metrics = defaultdict(lambda: [])
#         for size in sizes:
#             print(f"Setting target with size: {int(size)}")
#             self.nrows = size
#             self._df = self.build_df()
#             dt = 0
#             for incremental in [True, False]:
#
#                 if incremental:
#                     for irun in range(nruns):
#                         dt = stocks.append(incremental)
#                         self.metrics["method"].append("append")
#                         self.metrics["irun"].append(irun)
#                         self.metrics["is_incremental"].append(incremental)
#                         self.metrics["size"].append(size)
#                         self.metrics["duration"].append(dt)
#
#                 if not incremental:
#                     for irun in range(nruns):
#                         dt = stocks.overwrite(incremental)
#                         self.metrics["method"].append("overwrite")
#                         self.metrics["irun"].append(irun)
#                         self.metrics["is_incremental"].append(incremental)
#                         self.metrics["size"].append(size)
#                         self.metrics["duration"].append(dt)
#
#                 for irun in range(nruns):
#                     dt = stocks.merge(incremental)
#                     self.metrics["method"].append("merge")
#                     self.metrics["irun"].append(irun)
#                     self.metrics["is_incremental"].append(incremental)
#                     self.metrics["size"].append(size)
#                     self.metrics["duration"].append(dt)
#
#                 pd.DataFrame(self.metrics).to_csv("/Volumes/q01/sources/input/tmp/metrics.csv", index=False)
#
# stocks = Stocks(nrows=1e4, nstocks=500)


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

df = build_df()

write_target(df)

dfs = get_source(df)

merge(dfs)



# df = spark.read.table("q01.envcan.config_station")

# df.printSchema()
#
# # Merge Run
# import os
#
# source = models.MemoryDataSource(
#     df=stocks.get_source(),
# )
#
#
# class _Sink(models.FileDataSink):
#     mode: Literal["OVERWRITE", "APPEND", "IGNORE", "ERROR", "COMPLETE", "MERGE", None] = None
#
#     def _write_spark(self, df, mode=None) -> None:
#
#         if self.format in ["EXCEL"]:
#             raise ValueError(f"'{self.format}' format is not supported with Spark")
#
#         # Set mode
#         if mode is None:
#             mode = self.mode
#         if mode == "MERGE" and not os.path.exists(self.path):
#             mode = "OVERWRITE"
#
#         # Default Options
#         _options = {"mergeSchema": "true", "overwriteSchema": "false"}
#         if mode in ["OVERWRITE", "COMPLETE"]:
#             _options["mergeSchema"] = "false"
#             _options["overwriteSchema"] = "true"
#
#         if df.isStreaming:
#             _options["checkpointLocation"] = self._checkpoint_location
#
#         # User Options
#         for k, v in self.write_options.items():
#             _options[k] = v
#
#         # Merge
#         if mode == "MERGE":
#             table = DeltaTable.forPath(spark, self.path)
#
#         (
#             table.alias("target")
#             .merge(
#                 source.alias("source"),
#                 # how="outer",
#                 " AND ".join([f"source.{c} = target.{c}" for c in self.keys]),
#             )
#             .whenMatchedUpdate(
#                 set={f"target.{c}": f"source.{c}" for c in ["open", "close", "high", "low", "_from"]},
#                 condition="source._is_updated = true",
#             )
#             .whenNotMatchedInsert(
#                 # values = {f"target.{c}": f"source.{c}" for c in df.columns if not c.startswith("_")}
#                 values={f"target.{c}": f"source.{c}" for c in source.columns}
#             )
#         ).execute()
#
#         t = "static"
#         if df.isStreaming:
#             t = "stream"
#             writer = df.writeStream.trigger(availableNow=True)
#
#         else:
#             writer = df.write
#
#         print(
#             f"Writing df as {t} {self.format} to {self.path} with mode {mode} and options {_options}"
#         )
#         writer.mode(mode).format(self.format).options(**_options).save(self.path)
#
#
# sink = _Sink(
#     path=stocks.path,
#     format="DELTA",
#     mode="MERGE",
# )
#
# node = models.PipelineNode(
#     source=source,
#     transformer=models.SparkChain(nodes=[]),
#     # sink=sink,
# )
#
# node.execute()
#
# # def merge(self, incremental=True):
# #     _k1 = "increment" if incremental else "dump"
# #     self.write_target()
# #     source = self.get_source(incremental)
# #     t0 = time.time()
# #     print(f"Processing {_k1} as merge...", end="")
# #     table_target = DeltaTable.forPath(spark, self.path)
# #     (
# #         table_target.alias("target")
# #         .merge(
# #             source.alias("source"),
# #             # how="outer",
# #             " AND ".join([f"source.{c} = target.{c}" for c in self.keys]),
# #         )
# #         .whenMatchedUpdate(
# #             set={f"target.{c}": f"source.{c}" for c in ["open", "close", "high", "low", "_from"]},
# #             condition="source._is_updated = true",
# #         )
# #         .whenNotMatchedInsert(
# #             # values = {f"target.{c}": f"source.{c}" for c in df.columns if not c.startswith("_")}
# #             values = {f"target.{c}": f"source.{c}" for c in source.columns}
# #         )
# #     ).execute()
# #     dt = print_done(t0)
# #     return dt
#
