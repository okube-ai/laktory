from pathlib import Path


class DataFramesFactory:
    def __init__(self, spark_factory):
        self._brz = None
        self._slv = None
        self._slv_polars = None
        self._slv_stream = None
        self._meta = None
        self._meta_polars = None
        self._name = None
        self._spark = None
        self.data_dirpath = Path(__file__).parent / "../../tests/data/"
        self.spark_factory = spark_factory

    @property
    def spark(self):
        if self._spark is None:
            self._spark = self.spark_factory.spark
        return self._spark

    @property
    def brz(self):
        if self._brz is None:
            self._brz = self.spark.read.parquet(
                str(self.data_dirpath / "brz_stock_prices")
            )
        return self._brz

    @property
    def slv(self):
        if self._slv is None:
            self._slv = self.spark.read.parquet(
                str(self.data_dirpath / "slv_stock_prices")
            )
        return self._slv

    @property
    def slv_stream(self):
        if self._slv_stream is None:
            self._slv_stream = (
                self.spark.readStream.format("delta")
                .option("startingOffsets", "earliest")
                .load(str(self.data_dirpath / "slv_stock_prices_delta"))
            )
        return self._slv_stream

    @property
    def meta(self):
        if self._meta is None:
            self._meta = self.spark.read.parquet(
                str(self.data_dirpath / "slv_stock_meta")
            )
        return self._meta

    @property
    def name(self):
        if self._name is None:
            import pandas as pd

            self._name = self.spark.createDataFrame(
                pd.DataFrame(
                    {
                        "symbol3": ["AAPL", "GOOGL", "AMZN"],
                        "name": ["Apple", "Google", "Amazon"],
                    }
                )
            )
        return self._name

    @property
    def slv_polars(self):
        if self._slv_polars is None:
            import polars as pl

            dirpath = self.data_dirpath / "slv_stock_prices"
            for filename in dirpath.iterdir():
                if str(filename).endswith(".parquet"):
                    break
            self._slv_polars = pl.read_parquet(dirpath / filename)
        return self._slv_polars

    @property
    def meta_polars(self):
        if self._meta_polars is None:
            import polars as pl

            dirpath = self.data_dirpath / "slv_stock_meta"
            for filename in dirpath.iterdir():
                if str(filename).endswith(".parquet"):
                    break
            self._meta_polars = pl.read_parquet(dirpath / filename)
        return self._meta_polars
