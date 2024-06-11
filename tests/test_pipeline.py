import os
import shutil
import networkx as nx
import polars

from laktory import models
from laktory._testing import spark
from laktory._testing import Paths

paths = Paths(__file__)

OPEN_FIGURES = False


with open(os.path.join(paths.data, "pl-spark-local.yaml"), "r") as fp:
    pl = models.Pipeline.model_validate_yaml(fp)


with open(os.path.join(paths.data, "pl-polars-local.yaml"), "r") as fp:
    pl_polars = models.Pipeline.model_validate_yaml(fp)


with open(os.path.join(paths.data, "pl-spark-dlt.yaml"), "r") as fp:
    pl_dlt = models.Pipeline.model_validate_yaml(fp)  # also used in test_stack


# Update paths
slv_sink_path = os.path.join(paths.tmp, "pl_slv_sink")
gld_sink_path = os.path.join(paths.tmp, "pl_gld_sink")
for _pl in [pl, pl_polars]:
    _pl.nodes[0].source.path = os.path.join(paths.data, "brz_stock_prices")
    _pl.nodes[3].source.path = os.path.join(paths.data, "slv_stock_meta")
    _pl.nodes[1].sink.path = os.path.join(paths.tmp, slv_sink_path)
    _pl.nodes[2].sink.path = os.path.join(paths.tmp, gld_sink_path)
    if _pl.name == pl_polars.name:
        for i in [0, 3]:
            _dirpath = _pl.nodes[i].source.path
            filenames = [
                filename
                for filename in os.listdir(_dirpath)
                if filename.endswith(".parquet")
            ]
            _pl.nodes[i].source.path = os.path.join(_dirpath, filenames[0])

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
    assert (
        node_slv.transformer.nodes[-1].parsed_func_kwargs["other"].value.node
        == node_meta
    )

    # Test figure
    fig = pl.dag_figure()
    fig.write_html(os.path.join(paths.tmp, "dag.html"), auto_open=OPEN_FIGURES)


def test_children():

    for pn in pl.nodes:
        assert pn._parent == pl
        assert pn.source._parent == pn
        if pn.transformer is not None:
            assert pn.transformer._parent == pn
        if pn.sink is not None:
            assert pn.sink._parent == pn

        if pn.transformer:
            for tn in pn.transformer.nodes:
                assert tn._parent == pn.transformer

        for s in pn.get_sources():
            assert s._parent == pn


def test_execute():

    pl.execute(spark)

    # In memory DataFrames
    assert pl.nodes_dict["brz_stock_prices"].output_df.columns == [
        "name",
        "description",
        "producer",
        "data",
        "_bronze_at",
    ]
    assert pl.nodes_dict["brz_stock_prices"].output_df.count() == 80
    assert pl.nodes_dict["slv_stock_meta"].output_df.columns == [
        "symbol2",
        "currency",
        "first_traded",
        "_silver_at",
    ]
    assert pl.nodes_dict["slv_stock_meta"].output_df.count() == 3
    assert pl.nodes_dict["slv_stock_prices"].output_df.columns == [
        "_bronze_at",
        "created_at",
        "close",
        "currency",
        "first_traded",
        "_silver_at",
        "symbol",
    ]
    assert pl.nodes_dict["slv_stock_prices"].output_df.count() == 80
    assert pl.nodes_dict["gld_max_stock_prices"].output_df.columns == [
        "symbol",
        "max_price",
        "min_price",
        "_gold_at",
    ]
    assert pl.nodes_dict["gld_max_stock_prices"].output_df.count() == 4

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


def test_execute_polars():

    _pl = pl_polars

    # Check dataframe type assignment
    assert _pl.dataframe_type == "POLARS"
    assert _pl.user_dftype == "POLARS"
    for node in _pl.nodes:
        assert node.dataframe_type == "SPARK"
        assert node.user_dftype == "POLARS"
        assert node.source.dataframe_type == "SPARK"
        assert node.source.user_dftype == "POLARS"
        assert node.source.dftype == "POLARS"
        for s in node.get_sources():
            assert s.dataframe_type == "SPARK"
            assert s.user_dftype == "POLARS"
            assert s.dftype == "POLARS"

    _pl.execute()

    # In memory DataFrames
    assert _pl.nodes_dict["brz_stock_prices"].output_df.columns == [
        "name",
        "description",
        "producer",
        "data",
        "_bronze_at",
    ]
    assert _pl.nodes_dict["brz_stock_prices"].output_df.height == 80
    assert _pl.nodes_dict["slv_stock_meta"].output_df.columns == [
        "symbol2",
        "currency",
        "first_traded",
        "_silver_at",
    ]
    assert _pl.nodes_dict["slv_stock_meta"].output_df.height == 3
    print(_pl.nodes_dict["slv_stock_prices"].output_df.columns)
    assert _pl.nodes_dict["slv_stock_prices"].output_df.columns == [
        "_bronze_at",
        "created_at",
        "symbol",
        "close",
        "currency",
        "first_traded",
        "_silver_at",
    ]
    assert _pl.nodes_dict["slv_stock_prices"].output_df.height == 80
    assert _pl.nodes_dict["gld_max_stock_prices"].output_df.columns == [
        "symbol",
        "max_price",
        "min_price",
        "_gold_at",
    ]
    assert _pl.nodes_dict["gld_max_stock_prices"].output_df.height == 4

    # Sinks
    _df_slv = polars.read_parquet(slv_sink_path)
    _df_gld = polars.read_parquet(gld_sink_path)
    assert _df_slv.columns == [
        "_bronze_at",
        "created_at",
        "symbol",
        "close",
        "currency",
        "first_traded",
        "_silver_at",
    ]
    assert _df_slv.height == 80
    assert _df_gld.columns == ["symbol", "max_price", "min_price", "_gold_at"]
    assert _df_gld.height == 4

    # Cleanup
    os.remove(slv_sink_path)
    os.remove(gld_sink_path)


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
        "limit": None,
        "renames": None,
        "sample": None,
        "selects": None,
        "watermark": None,
        "catalog_name": "dev",
        "table_name": "brz_stock_prices",
        "schema_name": "sandbox",
        "warehouse": "DATABRICKS",
    }

    data = pl_dlt.model_dump()
    print(data)
    assert data == {
        "databricks_job": None,
        "dataframe_type": "SPARK",
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
                "dataframe_type": "SPARK",
                "description": None,
                "drop_duplicates": None,
                "drop_source_columns": False,
                "transformer": None,
                "expectations": [],
                "layer": "BRONZE",
                "name": "brz_stock_prices",
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
                    "limit": None,
                    "renames": None,
                    "sample": None,
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
                "dataframe_type": "SPARK",
                "description": None,
                "drop_duplicates": None,
                "drop_source_columns": True,
                "transformer": {
                    "nodes": [
                        {
                            "func_args": [],
                            "func_kwargs": {},
                            "func_name": None,
                            "sql_expr": None,
                            "with_column": {
                                "name": "created_at",
                                "type": "timestamp",
                                "unit": None,
                                "expr": None,
                                "sql_expr": "data.created_at",
                            },
                            "with_columns": [],
                        },
                        {
                            "func_args": [],
                            "func_kwargs": {},
                            "func_name": None,
                            "sql_expr": None,
                            "with_column": {
                                "name": "symbol",
                                "type": "string",
                                "unit": None,
                                "expr": None,
                                "sql_expr": "data.symbol",
                            },
                            "with_columns": [],
                        },
                        {
                            "func_args": [],
                            "func_kwargs": {},
                            "func_name": None,
                            "sql_expr": None,
                            "with_column": {
                                "name": "close",
                                "type": "double",
                                "unit": None,
                                "expr": None,
                                "sql_expr": "data.close",
                            },
                            "with_columns": [],
                        },
                        {
                            "func_args": ["data", "producer", "name", "description"],
                            "func_kwargs": {},
                            "func_name": "drop",
                            "sql_expr": None,
                            "with_column": None,
                            "with_columns": [],
                        },
                    ],
                    "spark": True,
                },
                "expectations": [],
                "layer": "SILVER",
                "name": "slv_stock_prices",
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
                    "limit": None,
                    "renames": None,
                    "sample": None,
                    "selects": None,
                    "watermark": None,
                    "node_name": "brz_stock_prices",
                },
                "timestamp_key": None,
            },
        ],
        "orchestrator": "DLT",
        "udfs": [],
    }

    # Test resources
    resources = pl_dlt.core_resources
    assert len(resources) == 4

    wsf = resources[0]
    wsfp = resources[1]
    dlt = resources[2]
    dltp = resources[3]

    assert isinstance(wsf, models.resources.databricks.WorkspaceFile)
    assert isinstance(wsfp, models.resources.databricks.Permissions)
    assert isinstance(dlt, models.resources.databricks.DLTPipeline)
    assert isinstance(dltp, models.resources.databricks.Permissions)

    assert dlt.resource_name == "dlt-pl-stock-prices"
    assert dltp.resource_name == "permissions-dlt-pl-stock-prices"
    assert wsf.resource_name == "workspace-file-laktory-pipelines-pl-stock-prices-json"
    assert (
        wsfp.resource_name
        == "permissions-workspace-file-laktory-pipelines-pl-stock-prices-json"
    )

    assert dlt.options.provider == "${resources.databricks2}"
    assert dltp.options.provider == "${resources.databricks2}"
    assert wsf.options.provider == "${resources.databricks1}"
    assert wsfp.options.provider == "${resources.databricks1}"

    assert dlt.options.depends_on == []
    assert dltp.options.depends_on == ["${resources.dlt-pl-stock-prices}"]
    assert wsf.options.depends_on == []
    assert wsfp.options.depends_on == [
        "${resources.workspace-file-laktory-pipelines-pl-stock-prices-json}"
    ]


def test_pipeline_job():

    with open(os.path.join(paths.data, "pl-spark-job.yaml"), "r") as fp:
        pl = models.Pipeline.model_validate_yaml(fp)

    # Test job
    job = pl.databricks_job
    data = job.model_dump(exclude_none=True)
    print(data)
    assert data == {
        "access_controls": [],
        "clusters": [
            {
                "data_security_mode": "USER_ISOLATION",
                "init_scripts": [],
                "name": "node-cluster",
                "node_type_id": "Standard_DS3_v2",
                "spark_conf": {},
                "spark_env_vars": {},
                "spark_version": "14.0.x-scala2.12",
                "ssh_public_keys": [],
            }
        ],
        "name": "job-pl-stock-prices",
        "parameters": [{"name": "pipeline_name", "default": "pl-stock-prices"}],
        "tags": {},
        "tasks": [
            {
                "depends_ons": [],
                "job_cluster_key": "node-cluster",
                "libraries": [{"pypi": {"package": "laktory==0.3.0"}}],
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "brz_stock_prices"},
                },
                "task_key": "node-brz_stock_prices",
            },
            {
                "depends_ons": [],
                "job_cluster_key": "node-cluster",
                "libraries": [{"pypi": {"package": "laktory==0.3.0"}}],
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "slv_stock_meta"},
                },
                "task_key": "node-slv_stock_meta",
            },
            {
                "depends_ons": [
                    {"task_key": "node-brz_stock_prices"},
                    {"task_key": "node-slv_stock_meta"},
                ],
                "job_cluster_key": "node-cluster",
                "libraries": [{"pypi": {"package": "laktory==0.3.0"}}],
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "slv_stock_prices"},
                },
                "task_key": "node-slv_stock_prices",
            },
        ],
        "laktory_version": "0.3.0",
    }

    # Test resources
    resources = pl.core_resources
    assert len(resources) == 3


if __name__ == "__main__":
    # test_dag()
    # test_children()
    # test_execute()
    test_execute_polars()
    # test_pipeline_dlt()
    # test_pipeline_job()
