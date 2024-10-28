import os
import io
from pathlib import Path
import shutil
import uuid
import networkx as nx
import pandas as pd
import polars
from pyspark.sql import Window
import pyspark.sql.functions as F

from laktory import models
from laktory._testing import spark
from laktory._testing import Paths
from laktory._testing import df_brz

paths = Paths(__file__)

OPEN_FIGURES = False

testdir_path = Path(__file__).parent


def get_pl():
    pl_path = testdir_path / "tmp" / "test_pipeline" / str(uuid.uuid4())

    with open(os.path.join(paths.data, "pl-polars-local.yaml"), "r") as fp:
        data = fp.read()
        data = data.replace("{data_dir}", str(testdir_path / "data"))
        data = data.replace("{pl_dir}", str(pl_path))
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))

    return pl, pl_path


gld_target = pd.DataFrame({
    "symbol": ["AAPL", "GOOGL", "MSFT"],
    "max_price": [190.0,  138.0, 330.0],
    "min_price": [170.0,  129.0, 312.0],
    "mean_price": [177.0,  134.0, 320.0],
})


def test_execute():

    pl = get_pl()

    # Check dataframe type assignment
    assert _pl.dataframe_type == "POLARS"
    for node in _pl.nodes:
        assert node.dataframe_type == "POLARS"
        assert node.source.dataframe_type == "POLARS"
        for s in node.get_sources():
            assert s.dataframe_type == "POLARS"

    _pl.execute()
    #
    # # In memory DataFrames
    # assert _pl.nodes_dict["brz_stock_prices"].output_df.columns == [
    #     "name",
    #     "description",
    #     "producer",
    #     "data",
    #     "_bronze_at",
    # ]
    # assert _pl.nodes_dict["brz_stock_prices"].output_df.height == 80
    # assert _pl.nodes_dict["slv_stock_meta"].output_df.columns == [
    #     "symbol2",
    #     "currency",
    #     "first_traded",
    #     "_silver_at",
    # ]
    # assert _pl.nodes_dict["slv_stock_meta"].output_df.height == 3
    # assert _pl.nodes_dict["slv_stock_prices"].output_df.columns == [
    #     "_bronze_at",
    #     "created_at",
    #     "symbol",
    #     "close",
    #     "currency",
    #     "first_traded",
    #     "_silver_at",
    # ]
    # assert _pl.nodes_dict["slv_stock_prices"].output_df.height == 80
    # assert _pl.nodes_dict["gld_max_stock_prices"].output_df.columns == [
    #     "symbol",
    #     "max_price",
    #     "min_price",
    #     "_gold_at",
    # ]
    # assert _pl.nodes_dict["gld_max_stock_prices"].output_df.height == 4
    #
    # # Sinks
    # _df_slv = polars.read_parquet(slv_sink_path)
    # _df_gld = polars.read_parquet(gld_sink_path)
    # assert _df_slv.columns == [
    #     "_bronze_at",
    #     "created_at",
    #     "symbol",
    #     "close",
    #     "currency",
    #     "first_traded",
    #     "_silver_at",
    # ]
    # assert _df_slv.height == 80
    # assert _df_gld.columns == ["symbol", "max_price", "min_price", "_gold_at"]
    # assert _df_gld.height == 4
    #
    # # Cleanup
    # os.remove(brz_sink_path)
    # os.remove(slv_sink_path)
    # os.remove(gld_sink_path)
    # os.remove(meta_sink_path)


def test_execute_polars_sql():

    _pl = pl_polars2.model_copy()

    # Select join node
    node = _pl.nodes_dict["slv_stock_prices"]
    t3 = node.transformer.nodes[3]

    t3.sql_expr = """
    SELECT
        *
    FROM
        {df} as df
    LEFT JOIN
        {nodes.slv_stock_meta} as meta
    ON df.symbol = meta.symbol2
    ;
    """

    _pl.execute(spark=spark, write_sinks=False)
    df = node.output_df
    assert df.columns == [
        "_bronze_at",
        "created_at",
        "symbol",
        "close",
        "symbol2",
        "currency",
        "first_traded",
        "_silver_at",
    ]


if __name__ == "__main__":
    test_execute()
    # test_execute_polars_sql()
