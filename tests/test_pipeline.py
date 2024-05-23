import os
import shutil
import networkx as nx

from laktory import models
from laktory._testing import spark
from laktory._testing import Paths

paths = Paths(__file__)

OPEN_FIGURES = False


with open(os.path.join(paths.data, "pl-spark-local.yaml"), "r") as fp:
    pl = models.Pipeline.model_validate_yaml(fp)


with open(os.path.join(paths.data, "pl-dlt.yaml"), "r") as fp:
    pl_dlt = models.Pipeline.model_validate_yaml(fp)  # also used in test_stack


# Update paths
slv_sink_path = os.path.join(paths.tmp, "pl_slv_sink")
gld_sink_path = os.path.join(paths.tmp, "pl_gld_sink")
pl.nodes[0].source.path = os.path.join(paths.data, "brz_stock_prices")
pl.nodes[3].source.path = os.path.join(paths.data, "slv_stock_meta")
pl.nodes[1].sink.path = os.path.join(paths.tmp, slv_sink_path)
pl.nodes[2].sink.path = os.path.join(paths.tmp, gld_sink_path)

# Save nodes
node_brz = pl.nodes[0]
node_slv = pl.nodes[1]
node_gld = pl.nodes[2]
node_meta = pl.nodes[3]


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
    assert pl.sorted_nodes == [node_brz, node_meta, node_slv, node_gld]
    assert node_slv.source.node == node_brz
    assert node_gld.source.node == node_slv
    assert node_slv.chain.nodes[-1].spark_func_kwargs["other"].value.node == node_meta

    # Test figure
    fig = pl.dag_figure()
    fig.write_html(os.path.join(paths.tmp, "dag.html"), auto_open=OPEN_FIGURES)


def test_execute():
    pl.execute(spark)

    # In memory DataFrames
    assert pl.nodes_dict["brz_stock_prices"]._output_df.columns == [
        "name",
        "description",
        "producer",
        "data",
        "_bronze_at",
    ]
    assert pl.nodes_dict["brz_stock_prices"]._output_df.count() == 80
    assert pl.nodes_dict["slv_stock_meta"]._output_df.columns == [
        "symbol2",
        "currency",
        "first_traded",
        "_silver_at",
    ]
    assert pl.nodes_dict["slv_stock_meta"]._output_df.count() == 3
    assert pl.nodes_dict["slv_stock_prices"]._output_df.columns == [
        "_bronze_at",
        "created_at",
        "close",
        "currency",
        "first_traded",
        "_silver_at",
        "symbol",
    ]
    assert pl.nodes_dict["slv_stock_prices"]._output_df.count() == 80
    assert pl.nodes_dict["gld_max_stock_prices"]._output_df.columns == [
        "symbol",
        "max_price",
        "min_price",
        "_gold_at",
    ]
    assert pl.nodes_dict["gld_max_stock_prices"]._output_df.count() == 4

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


def test_pipeline_dlt():

    # Test Sink as Source
    sink_source = pl_dlt.nodes[1].source.node.sink.as_source(
        as_stream=pl_dlt.nodes[1].source.as_stream
    )
    data = sink_source.model_dump()
    print(data)
    assert data == {
        "as_stream": True,
        "broadcast": False,
        "cdc": None,
        "dataframe_type": "SPARK",
        "drops": None,
        "filter": None,
        "renames": None,
        "selects": None,
        "watermark": None,
        "catalog_name": "dev",
        "from_dlt": None,
        "table_name": "brz_stock_prices",
        "schema_name": "sandbox",
        "warehouse": "DATABRICKS",
    }

    data = pl_dlt.model_dump()
    print(data)
    assert data == {
        "dlt": {
            "access_controls": [
                {
                    "group_name": "account users",
                    "permission_level": "CAN_VIEW",
                    "service_principal_name": None,
                    "user_name": None,
                }
            ],
            "allow_duplicate_names": None,
            "catalog": "dev",
            "channel": "PREVIEW",
            "clusters": [],
            "configuration": {},
            "continuous": None,
            "development": None,
            "edition": None,
            "libraries": None,
            "name": "pl-stock-prices",
            "notifications": [],
            "photon": None,
            "serverless": None,
            "storage": None,
            "target": "sandbox",
        },
        "name": "pl-stock-prices",
        "nodes": [
            {
                "add_layer_columns": True,
                "dlt_template": "DEFAULT",
                "description": None,
                "drop_duplicates": None,
                "drop_source_columns": False,
                "chain": None,
                "expectations": [],
                "id": "brz_stock_prices",
                "layer": "BRONZE",
                "primary_key": None,
                "sink": {
                    "mode": None,
                    "catalog_name": "dev",
                    "checkpoint_location": None,
                    "format": "DELTA",
                    "schema_name": "sandbox",
                    "table_name": "brz_stock_prices",
                    "warehouse": "DATABRICKS",
                },
                "source": {
                    "as_stream": True,
                    "broadcast": False,
                    "cdc": None,
                    "dataframe_type": "SPARK",
                    "drops": None,
                    "filter": None,
                    "renames": None,
                    "selects": None,
                    "watermark": None,
                    "format": "JSON",
                    "header": True,
                    "multiline": False,
                    "path": "/Volumes/dev/sources/landing/events/yahoo-finance/stock_price/",
                    "read_options": {},
                    "schema_location": None,
                },
                "timestamp_key": None,
            },
            {
                "add_layer_columns": True,
                "dlt_template": "DEFAULT",
                "description": None,
                "drop_duplicates": None,
                "drop_source_columns": True,
                "chain": {
                    "nodes": [
                        {
                            "allow_missing_column_args": False,
                            "column": {
                                "name": "created_at",
                                "type": "timestamp",
                                "unit": None,
                            },
                            "spark_func_args": [],
                            "spark_func_kwargs": {},
                            "spark_func_name": None,
                            "sql_expression": "data.created_at",
                        },
                        {
                            "allow_missing_column_args": False,
                            "column": {
                                "name": "symbol",
                                "type": "string",
                                "unit": None,
                            },
                            "spark_func_args": [{"value": "data.symbol"}],
                            "spark_func_kwargs": {},
                            "spark_func_name": "coalesce",
                            "sql_expression": None,
                        },
                        {
                            "allow_missing_column_args": False,
                            "column": {"name": "close", "type": "double", "unit": None},
                            "spark_func_args": [],
                            "spark_func_kwargs": {},
                            "spark_func_name": None,
                            "sql_expression": "data.close",
                        },
                        {
                            "allow_missing_column_args": False,
                            "column": None,
                            "spark_func_args": [
                                {"value": "data"},
                                {"value": "producer"},
                                {"value": "name"},
                                {"value": "description"},
                            ],
                            "spark_func_kwargs": {},
                            "spark_func_name": "drop",
                            "sql_expression": None,
                        },
                    ]
                },
                "expectations": [],
                "id": "slv_stock_prices",
                "layer": "SILVER",
                "primary_key": None,
                "sink": {
                    "mode": None,
                    "catalog_name": "dev",
                    "checkpoint_location": None,
                    "format": "DELTA",
                    "schema_name": "sandbox",
                    "table_name": "slv_stock_prices",
                    "warehouse": "DATABRICKS",
                },
                "source": {
                    "as_stream": True,
                    "broadcast": False,
                    "cdc": None,
                    "dataframe_type": "SPARK",
                    "drops": None,
                    "filter": None,
                    "renames": None,
                    "selects": None,
                    "watermark": None,
                    "node_id": "brz_stock_prices",
                },
                "timestamp_key": None,
            },
        ],
        "engine": "DLT",
        "udfs": [],
    }

    # Test resources
    resources = pl_dlt.core_resources
    assert len(resources) == 4

    assert isinstance(resources[0], models.resources.databricks.DLTPipeline)
    assert isinstance(resources[1], models.resources.databricks.Permissions)
    assert isinstance(resources[2], models.resources.databricks.WorkspaceFile)
    assert isinstance(resources[3], models.resources.databricks.Permissions)

    assert resources[0].resource_name == "dlt-pl-stock-prices"
    assert resources[1].resource_name == "permissions-dlt-pl-stock-prices"
    assert (
        resources[2].resource_name
        == "workspace-file-laktory-pipelines-pl-stock-prices-json"
    )
    assert (
        resources[3].resource_name
        == "permissions-workspace-file-laktory-pipelines-pl-stock-prices-json"
    )

    assert resources[0].options.provider == "${resources.databricks2}"
    assert resources[1].options.provider == "${resources.databricks2}"
    assert resources[2].options.provider == "${resources.databricks1}"
    assert resources[3].options.provider == "${resources.databricks1}"

    assert resources[0].options.depends_on == []
    assert resources[1].options.depends_on == ["${resources.dlt-pl-stock-prices}"]
    assert resources[2].options.depends_on == []
    assert resources[3].options.depends_on == [
        "${resources.workspace-file-laktory-pipelines-pl-stock-prices-json}"
    ]


if __name__ == "__main__":
    test_dag()
    test_execute()
    test_pipeline_dlt()
