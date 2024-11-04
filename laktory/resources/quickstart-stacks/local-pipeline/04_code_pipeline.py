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
        multiline=True,
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
    transformer=models.PolarsChain(
        nodes=[
            models.PolarsChainNode(
                sql_expr="""
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
            models.PolarsChainNode(
                func_name="unique",
                func_kwargs={
                    "subset": ["symbol", "created_at"],
                    "keep": "first",
                },
            ),
        ]
    ),
)


# --------------------------------------------------------------------------- #
# Gold Node                                                                   #
# --------------------------------------------------------------------------- #


def process_stocks(df):
    import polars as pl

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
    transformer=models.PolarsChain(
        nodes=[
            models.PolarsChainNode(
                func_name="process_stocks",
            ),
        ]
    ),
)


# --------------------------------------------------------------------------- #
# Execute Pipeline                                                            #
# --------------------------------------------------------------------------- #

pipeline = models.Pipeline(
    name="pl-stock-prices",
    dataframe_type="POLARS",
    nodes=[
        node_brz,
        node_slv,
        node_gld,
    ],
)

# Run
pipeline.execute(udfs=[process_stocks])
