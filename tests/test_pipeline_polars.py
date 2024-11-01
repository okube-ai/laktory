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


def get_pl(clean_path=False):
    pl_path = testdir_path / "tmp" / "test_pipeline_polars" / str(uuid.uuid4())

    with open(os.path.join(paths.data, "pl-polars-local.yaml"), "r") as fp:
        data = fp.read()
        data = data.replace("{data_dir}", str(testdir_path / "data"))
        data = data.replace("{pl_dir}", str(pl_path))
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))

    if clean_path and os.path.exists(str(pl_path)):
        shutil.rmtree(str(pl_path))

    return pl, pl_path


gld_target = pd.DataFrame(
    {
        "symbol": ["AAPL", "GOOGL", "MSFT"],
        "max_price": [190.0, 138.0, 330.0],
        "min_price": [170.0, 129.0, 312.0],
        "mean_price": [177.0, 134.0, 320.0],
    }
)


def test_df_type():

    pl, _ = get_pl()

    # Check dataframe type assignment
    assert pl.dataframe_type == "POLARS"
    for node in pl.nodes:
        assert node.dataframe_type == "POLARS"
        assert node.source.dataframe_type == "POLARS"
        for s in node.get_sources():
            assert s.dataframe_type == "POLARS"


def test_execute():

    pl, pl_path = get_pl(clean_path=True)

    # Run
    pl.execute()

    # Test - Brz Stocks
    df = pl.nodes_dict["brz_stock_prices"].primary_sink.read().collect()
    assert df.columns == ["name", "description", "producer", "data", "_bronze_at"]
    assert df.height == 80

    # Test - Slv Meta
    df = pl.nodes_dict["slv_stock_meta"].output_df.collect()
    assert df.columns == ["symbol2", "currency", "first_traded"]
    assert df.height == 3

    # Test - Slv Stocks
    df = pl.nodes_dict["slv_stock_prices"].primary_sink.read().collect()
    assert df.columns == [
        "_bronze_at",
        "created_at",
        "symbol",
        "close",
        "currency",
        "first_traded",
        "_silver_at",
    ]
    assert df.height == 52

    # Test - Gold
    df = (
        pl.nodes_dict["gld_stock_prices"]
        .output_df.collect()
        .to_pandas()
        .round(0)
        .sort_values("symbol")
        .reset_index(drop=True)
    )
    assert len(df) == 3
    assert df.equals(gld_target)

    # Cleanup
    shutil.rmtree(pl_path)


def test_sql_join():

    # Get Pipeline
    pl, pl_path = get_pl(clean_path=True)

    # Update join
    node = pl.nodes_dict["slv_stock_prices"]
    t4 = node.transformer.nodes[-1]
    t4.sql_expr = """
    SELECT
        *
    FROM
        {df} as df
    LEFT JOIN
        {nodes.slv_stock_meta} as meta
    ON df.symbol = meta.symbol2
    ;
    """

    # Execute
    pl.execute(spark)

    # Test
    df = node.primary_sink.read().collect()
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

    # Cleanup
    shutil.rmtree(pl_path)


if __name__ == "__main__":
    test_df_type()
    test_execute()
    test_sql_join()
