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

pl = models.Pipeline(name="pl-stock-prices", nodes=[node_brz, node_slv, node_gld, node_meta_slv])


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


def test_pipeline_dlt(
        node_brz=node_brz,
        node_meta_slv=node_meta_slv,
        node_slv=node_slv,
        node_gld=node_gld,
):

    node_brz = node_slv.copy()
    node_brz.sink = models.TableDataSink(
        table_name="brz_sock_prices",
    )

    node_slv = node_slv.copy()
    node_slv.source = models.TableDataSource(
        table_name="brz_stock_prices",
        from_dlt=True,
    )
    node_slv.sink = models.TableDataSink(
        table_name="slv_stock_prices",
    )

    node_brz = node_slv.copy()
    node_slv.sink = models.TableDataSink(
        table_name="brz_sock_prices",
    )

    node_slv = node_slv.copy()
    node_slv.sink = models.TableDataSink(
        table_name="slv_sock_prices",
    )

    pl = models.Pipeline(
        name="pl-stock-prices",
        dlt=dict(
            catalog="dev1",
            target="markets1",
        ),
        nodes=[node_brz, node_slv, node_gld, node_meta_slv],
        engine="DLT",
        udfs=[
            {
                "module_name": "stock_functions",
                "function_name": "high",
            }
        ],
    )

    data = pl.model_dump()
    print(data)
    assert data == {'dlt': {'access_controls': [], 'allow_duplicate_names': None, 'catalog': 'dev1', 'channel': 'PREVIEW', 'clusters': [], 'configuration': {}, 'continuous': None, 'development': None, 'edition': None, 'libraries': None, 'name': 'pl-stock-prices', 'notifications': [], 'photon': None, 'serverless': None, 'storage': None, 'tables': [], 'target': 'markets1', 'udfs': []}, 'name': 'pl-stock-prices', 'nodes': [{'add_layer_columns': True, 'drop_duplicates': None, 'drop_source_columns': True, 'chain': {'nodes': [{'allow_missing_column_args': False, 'column': {'name': 'created_at', 'type': 'timestamp', 'unit': None}, 'spark_func_args': [], 'spark_func_kwargs': {}, 'spark_func_name': None, 'sql_expression': 'data.created_at'}, {'allow_missing_column_args': False, 'column': {'name': 'symbol', 'type': 'string', 'unit': None}, 'spark_func_args': [{'value': 'data.symbol'}], 'spark_func_kwargs': {}, 'spark_func_name': 'coalesce', 'sql_expression': None}, {'allow_missing_column_args': False, 'column': {'name': 'close', 'type': 'double', 'unit': None}, 'spark_func_args': [], 'spark_func_kwargs': {}, 'spark_func_name': None, 'sql_expression': 'data.close'}, {'allow_missing_column_args': False, 'column': None, 'spark_func_args': [{'value': 'data'}, {'value': 'producer'}, {'value': 'name'}, {'value': 'description'}], 'spark_func_kwargs': {}, 'spark_func_name': 'drop', 'sql_expression': None}, {'allow_missing_column_args': False, 'column': None, 'spark_func_args': [], 'spark_func_kwargs': {'other': {'value': {'as_stream': False, 'broadcast': False, 'cdc': None, 'dataframe_type': 'SPARK', 'drops': None, 'filter': None, 'renames': {'symbol2': 'symbol'}, 'selects': None, 'watermark': None, 'node_id': 'slv_stock_meta', 'node': None}}, 'on': {'value': ['symbol']}}, 'spark_func_name': 'smart_join', 'sql_expression': None}]}, 'expectations': [], 'id': 'slv_stock_prices', 'layer': 'SILVER', 'primary_key': None, 'sink': {'as_stream': False, 'mode': None, 'catalog_name': None, 'format': 'DELTA', 'schema_name': None, 'table_name': 'slv_stock_prices', 'warehouse': 'DATABRICKS'}, 'source': {'as_stream': False, 'broadcast': False, 'cdc': None, 'dataframe_type': 'SPARK', 'drops': None, 'filter': None, 'renames': None, 'selects': None, 'watermark': None, 'catalog_name': None, 'from_dlt': True, 'table_name': 'brz_stock_prices', 'schema_name': None, 'warehouse': 'DATABRICKS'}, 'timestamp_key': None}, {'add_layer_columns': True, 'drop_duplicates': None, 'drop_source_columns': True, 'chain': {'nodes': [{'allow_missing_column_args': False, 'column': {'name': 'created_at', 'type': 'timestamp', 'unit': None}, 'spark_func_args': [], 'spark_func_kwargs': {}, 'spark_func_name': None, 'sql_expression': 'data.created_at'}, {'allow_missing_column_args': False, 'column': {'name': 'symbol', 'type': 'string', 'unit': None}, 'spark_func_args': [{'value': 'data.symbol'}], 'spark_func_kwargs': {}, 'spark_func_name': 'coalesce', 'sql_expression': None}, {'allow_missing_column_args': False, 'column': {'name': 'close', 'type': 'double', 'unit': None}, 'spark_func_args': [], 'spark_func_kwargs': {}, 'spark_func_name': None, 'sql_expression': 'data.close'}, {'allow_missing_column_args': False, 'column': None, 'spark_func_args': [{'value': 'data'}, {'value': 'producer'}, {'value': 'name'}, {'value': 'description'}], 'spark_func_kwargs': {}, 'spark_func_name': 'drop', 'sql_expression': None}, {'allow_missing_column_args': False, 'column': None, 'spark_func_args': [], 'spark_func_kwargs': {'other': {'value': {'as_stream': False, 'broadcast': False, 'cdc': None, 'dataframe_type': 'SPARK', 'drops': None, 'filter': None, 'renames': {'symbol2': 'symbol'}, 'selects': None, 'watermark': None, 'node_id': 'slv_stock_meta', 'node': None}}, 'on': {'value': ['symbol']}}, 'spark_func_name': 'smart_join', 'sql_expression': None}]}, 'expectations': [], 'id': 'slv_stock_prices', 'layer': 'SILVER', 'primary_key': None, 'sink': {'as_stream': False, 'mode': None, 'catalog_name': None, 'format': 'DELTA', 'schema_name': None, 'table_name': 'slv_sock_prices', 'warehouse': 'DATABRICKS'}, 'source': {'as_stream': False, 'broadcast': False, 'cdc': None, 'dataframe_type': 'SPARK', 'drops': None, 'filter': None, 'renames': None, 'selects': None, 'watermark': None, 'catalog_name': None, 'from_dlt': True, 'table_name': 'brz_stock_prices', 'schema_name': None, 'warehouse': 'DATABRICKS'}, 'timestamp_key': None}, {'add_layer_columns': True, 'drop_duplicates': None, 'drop_source_columns': None, 'chain': {'nodes': [{'allow_missing_column_args': False, 'column': None, 'spark_func_args': [], 'spark_func_kwargs': {'groupby_columns': {'value': ['symbol']}, 'agg_expressions': {'value': [{'column': {'name': 'max_price', 'type': 'double'}, 'spark_func_name': 'max', 'spark_func_args': ['close']}, {'column': {'name': 'min_price', 'type': 'double'}, 'spark_func_name': 'min', 'spark_func_args': ['close']}]}}, 'spark_func_name': 'groupby_and_agg', 'sql_expression': None}]}, 'expectations': [], 'id': 'gld_max_stock_prices', 'layer': 'GOLD', 'primary_key': None, 'sink': {'as_stream': False, 'mode': 'OVERWRITE', 'checkpoint_location': None, 'format': 'PARQUET', 'path': '/Users/osoucy/Documents/sources/okube/laktory/tests/tmp/pl_gld_sink', 'write_options': {}}, 'source': {'as_stream': False, 'broadcast': False, 'cdc': None, 'dataframe_type': 'SPARK', 'drops': None, 'filter': None, 'renames': None, 'selects': None, 'watermark': None, 'node_id': 'slv_stock_prices', 'node': None}, 'timestamp_key': None}, {'add_layer_columns': True, 'drop_duplicates': None, 'drop_source_columns': True, 'chain': None, 'expectations': [], 'id': 'slv_stock_meta', 'layer': 'SILVER', 'primary_key': None, 'sink': None, 'source': {'as_stream': False, 'broadcast': False, 'cdc': None, 'dataframe_type': 'SPARK', 'drops': None, 'filter': None, 'renames': None, 'selects': None, 'watermark': None, 'format': 'JSON', 'header': True, 'multiline': False, 'path': 'dummy/path/', 'read_options': {}, 'schema_location': None}, 'timestamp_key': None}], 'engine': 'DLT', 'udfs': [{'module_name': 'stock_functions', 'function_name': 'high', 'module_path': None}]}


if __name__ == "__main__":
    test_dag()
    test_execute()
    # test_pipeline_dlt()
