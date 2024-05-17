import os
import shutil
import networkx as nx

from laktory import models
from laktory._testing import spark
from laktory._testing import Paths

paths = Paths(__file__)

# Data
df_brz = spark.read.parquet(os.path.join(paths.data, "./brz_stock_prices"))
df_slv = spark.read.parquet(os.path.join(paths.data, "./slv_stock_prices"))
df_slv_meta = spark.read.parquet(os.path.join(paths.data, "./slv_stock_meta"))

OPEN_FIGURES = False

slv_sink_path = os.path.join(paths.tmp, "pl_slv_sink")
gld_sink_path = os.path.join(paths.tmp, "pl_gld_sink")

node_brz = models.PipelineNode(
    id="brz_stock_prices",
    layer="BRONZE",
    source={
        "path": "dummy/path/",
        "mock_df": df_brz,
    },
)

node_meta_slv = models.PipelineNode(
    id="slv_stock_meta",
    layer="SILVER",
    source={
        "path": "dummy/path/",
        "mock_df": df_slv_meta,
    },
)

node_slv = models.PipelineNode(
    id="slv_stock_prices",
    layer="SILVER",
    source={
        "node_id": "brz_stock_prices",
    },
    chain={
        "nodes": [
            {
                "column": {"name": "created_at", "type": "timestamp"},
                "sql_expression": "data.created_at",
            },
            {
                "column": {"name": "symbol"},
                "spark_func_name": "coalesce",
                "spark_func_args": ["data.symbol"],
            },
            {
                "column": {"name": "close", "type": "double"},
                "sql_expression": "data.close",
            },
            {
                "spark_func_name": "drop",
                "spark_func_args": ["data", "producer", "name", "description"],
            },
            {
                "spark_func_name": "smart_join",
                "spark_func_kwargs": {
                    "other": {
                        "node_id": "slv_stock_meta",
                        "renames": {"symbol2": "symbol"},
                    },
                    "on": ["symbol"],
                },
            },
        ]
    },
    sink={
        "path": slv_sink_path,
        "format": "PARQUET",
        "mode": "OVERWRITE",
    },
)

node_gld = models.PipelineNode(
    id="gld_max_stock_prices",
    layer="GOLD",
    source={
        "node_id": "slv_stock_prices",
    },
    chain={
        "nodes": [
            {
                "spark_func_name": "groupby_and_agg",
                "spark_func_kwargs": {
                    "groupby_columns": ["symbol"],
                    "agg_expressions": [
                        {
                            "column": {"name": "max_price", "type": "double"},
                            "spark_func_name": "max",
                            "spark_func_args": ["close"],
                        },
                        {
                            "column": {"name": "min_price", "type": "double"},
                            "spark_func_name": "min",
                            "spark_func_args": ["close"],
                        },
                    ],
                },
            }
        ]
    },
    sink={
        "path": gld_sink_path,
        "format": "PARQUET",
        "mode": "OVERWRITE",
    },
)

pl = models.DataFramePipeline(nodes=[node_brz, node_slv, node_gld, node_meta_slv])


def test_dag():

    dag = pl.dag

    # Test Dag
    assert nx.is_directed_acyclic_graph(dag)
    assert len(dag.nodes) == 4
    assert len(dag.edges) == 3
    assert list(nx.topological_sort(dag)) == [
        "brz_stock_prices",
        "slv_stock_meta",
        "slv_stock_prices",
        "gld_max_stock_prices",
    ]

    # Test nodes assignment
    assert pl.sorted_nodes == [node_brz, node_meta_slv, node_slv, node_gld]
    assert node_slv.source.node == node_brz
    assert node_gld.source.node == node_slv
    assert (
        node_slv.chain.nodes[-1].spark_func_kwargs["other"].value.node == node_meta_slv
    )

    # Test figure
    fig = pl.dag_figure()
    fig.write_html(os.path.join(paths.tmp, "dag.html"), auto_open=OPEN_FIGURES)


def test_execute():
    pl.execute(spark)

    # In memory DataFrames
    assert pl.nodes_dict["brz_stock_prices"]._df.columns == [
        "name",
        "description",
        "producer",
        "data",
        "_bronze_at",
    ]
    assert pl.nodes_dict["brz_stock_prices"]._df.count() == 80
    assert pl.nodes_dict["slv_stock_meta"]._df.columns == [
        "symbol2",
        "currency",
        "first_traded",
        "_silver_at",
    ]
    assert pl.nodes_dict["slv_stock_meta"]._df.count() == 3
    assert pl.nodes_dict["slv_stock_prices"]._df.columns == [
        "_bronze_at",
        "created_at",
        "close",
        "currency",
        "first_traded",
        "_silver_at",
        "symbol",
    ]
    assert pl.nodes_dict["slv_stock_prices"]._df.count() == 80
    assert pl.nodes_dict["gld_max_stock_prices"]._df.columns == [
        "symbol",
        "max_price",
        "min_price",
        "_gold_at",
    ]
    assert pl.nodes_dict["gld_max_stock_prices"]._df.count() == 4

    # Sinks
    _df_slv = spark.read.format("PARQUET").load(slv_sink_path)
    _df_gld = spark.read.format("PARQUET").load(gld_sink_path)
    assert _df_slv.columns == [
        "_bronze_at",
        "created_at",
        "close",
        "currency",
        "first_traded",
        "_silver_at",
        "symbol",
    ]
    assert _df_slv.count() == 80
    assert _df_gld.columns == ["symbol", "max_price", "min_price", "_gold_at"]
    assert _df_gld.count() == 4

    # Cleanup
    shutil.rmtree(slv_sink_path)
    shutil.rmtree(gld_sink_path)


if __name__ == "__main__":
    test_dag()
    test_execute()
