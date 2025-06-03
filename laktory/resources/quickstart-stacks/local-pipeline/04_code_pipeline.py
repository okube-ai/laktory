import laktory as lk
from laktory import models

# --------------------------------------------------------------------------- #
# Code Pipeline                                                               #
# --------------------------------------------------------------------------- #

# While one of the main benefit of using Laktory is the ability to declare
# pipeline from a serialized format, there is nothing prevent from creating
# them directly in the code. Here is an example.

# --------------------------------------------------------------------------- #
# Bronze Node                                                                 #
# --------------------------------------------------------------------------- #

node_brz = models.PipelineNode(
    name="brz_stock_prices",
    source=models.FileDataSource(
        path="./data/stock_prices.json",
        format="JSONL",
    ),
    sinks=[
        models.FileDataSink(
            path="./data/brz_stock_prices.parquet",
            format="PARQUET",
        )
    ],
)


# --------------------------------------------------------------------------- #
# Silver Node                                                                 #
# --------------------------------------------------------------------------- #

node_slv = models.PipelineNode(
    name="slv_stock_prices",
    source=models.PipelineNodeDataSource(
        node_name="brz_stock_prices",
    ),
    sinks=[
        models.FileDataSink(
            path="./data/slv_stock_prices.parquet",
            format="PARQUET",
        )
    ],
    transformer=models.DataFrameTransformer(
        nodes=[
            models.DataFrameExpr(
                expr="""
                    SELECT
                      CAST(data.created_at AS TIMESTAMP) AS created_at,
                      data.symbol AS name,
                      data.symbol AS symbol,
                      data.open AS open,
                      data.close AS close,
                      data.high AS high,
                      data.low AS low,
                      data.volume AS volume
                    FROM
                      {df}                
                """
            ),
            models.DataFrameMethod(
                func_name="unique",
                func_kwargs={
                    "subset": ["symbol", "created_at"],
                    "keep": "any",
                },
            ),
        ]
    ),
)


# --------------------------------------------------------------------------- #
# Gold Node                                                                   #
# --------------------------------------------------------------------------- #


@lk.api.register_anyframe_namespace("custom")
class CustomNamespace:
    def __init__(self, _df):
        self._df = _df

    def process_stocks(self):
        import polars as pl

        df = self._df.to_native()  # convert from Narwhals to Polars

        df = df.with_columns(
            open_rounder=pl.col("open").round(2),
            close_rounded=pl.col("close").round(2),
        )

        return df


node_gld = models.PipelineNode(
    name="gld_stock_prices",
    source=models.PipelineNodeDataSource(
        node_name="slv_stock_prices",
    ),
    transformer=models.DataFrameTransformer(
        nodes=[
            models.DataFrameMethod(
                func_name="custom.process_stocks",
            ),
        ]
    ),
)


# --------------------------------------------------------------------------- #
# Execute Pipeline                                                            #
# --------------------------------------------------------------------------- #

pipeline = models.Pipeline(
    name="pl-stock-prices",
    dataframe_backend="POLARS",
    nodes=[
        node_brz,
        node_slv,
        node_gld,
    ],
)

# Run
pipeline.execute()
