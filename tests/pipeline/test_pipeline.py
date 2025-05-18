import io
from pathlib import Path

import networkx as nx
import pytest

from laktory import models
from laktory._testing import StreamingSource
from laktory._testing import assert_dfs_equal

# from laktory._testing import Paths

data_dirpath = Path(__file__).parent.parent / "data"

OPEN_FIGURES = False

#
# def get_pl(clean_path=False):
#     pl_path = testdir_path / "tmp" / "test_pipeline_spark" / str(uuid.uuid4())
#
#     with open(paths.data / "pl-spark-local.yaml", "r") as fp:
#         data = fp.read()
#         data = data.replace("{data_dir}", str(testdir_path / "data"))
#         data = data.replace("{pl_dir}", str(pl_path / "tables"))
#         pl = models.Pipeline.model_validate_yaml(io.StringIO(data))
#         pl.root_path = pl_path
#
#     if clean_path and os.path.exists(str(pl_path)):
#         shutil.rmtree(str(pl_path))
#
#     return pl, pl_path


def get_pl(tmp_path):
    with open(data_dirpath / "pl-new.yaml") as fp:
        data = fp.read()
        data = data.replace("{tmp_path}", str(tmp_path))
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))
        pl.root_path = tmp_path

    return pl


def get_brz(tmp_path, backend) -> models.PipelineNode:
    path = f"{tmp_path}/brz_sink/"
    mode = "OVERWRITE"
    if backend == "POLARS":
        path = path[:-1]
        mode = None

    return models.PipelineNode(
        name="brz",
        source={"format": "JSON", "path": f"{tmp_path}/brz_source/000.json"},
        sinks=[{"format": "PARQUET", "mode": mode, "path": path}],
    )


def get_slv(tmp_path, backend) -> models.PipelineNode:
    path = f"{tmp_path}/slv_sink/"
    mode = "OVERWRITE"
    if backend == "POLARS":
        path = path[:-1]
        mode = None

    return models.PipelineNode(
        name="slv",
        source={"node_name": "brz"},
        sinks=[{"format": "PARQUET", "mode": mode, "path": path}],
    )


#
#
# def get_source(pl_path):
#     source_path = str(pl_path / "tables/landing_stock_prices")
#     w = Window.orderBy("data.created_at", "data.symbol")
#     source = dff.brz.withColumn("index", F.row_number().over(w) - 1)
#
#     return source, source_path
#
#
# gld_target = pd.DataFrame(
#     {
#         "symbol": ["AAPL", "GOOGL", "MSFT"],
#         "max_price": [190.0, 138.0, 330.0],
#         "min_price": [170.0, 129.0, 312.0],
#         "mean_price": [177.0, 134.0, 320.0],
#     }
# )
#


def test_dag(tmp_path):
    pl = get_pl(tmp_path)

    dag = pl.dag

    # Test Dag
    assert nx.is_directed_acyclic_graph(dag)
    assert len(dag.nodes) == 2
    assert len(dag.edges) == 1
    assert list(nx.topological_sort(dag)) == [
        "brz",
        "slv",
    ]

    # # Test nodes assignment
    # assert [n.name for n in pl.sorted_nodes] == [
    #     "brz_stock_prices",
    #     "brz_stock_meta",
    #     "slv_stock_meta",
    #     "slv_stock_prices",
    #     "slv_stock_prices_tmp",
    #     "slv_stock_aapl",
    #     "slv_stock_msft",
    #     "gld_stock_prices",
    # ]
    # assert (
    #     pl.nodes_dict["slv_stock_prices"]
    #     .transformer.nodes[-1]
    #     .parsed_func_kwargs["other"]
    #     .value.node
    #     == pl.nodes_dict["slv_stock_meta"]
    # )
    #
    # # Test figure
    # fig = pl.dag_figure()
    # fig.write_html(paths.tmp / "dag.html", auto_open=OPEN_FIGURES)


def test_children(tmp_path):
    pl = get_pl(tmp_path)

    for pn in pl.nodes:
        assert pn.parent == pl
        assert pn.parent_pipeline == pl
        assert pn.source.parent == pn
        for s in pn.all_sinks:
            assert s.parent == pn

        if pn.transformer:
            assert pn.transformer.parent == pn
            assert pn.transformer.parent_pipeline == pl
            for tn in pn.transformer.nodes:
                assert tn.parent_pipeline == pl
                assert tn.parent_pipeline_node == pn
                assert tn.parent == pn.transformer

        for s in pn.data_sources:
            assert s.parent == pn
            assert s.parent_pipeline_node == pn
            assert s.parent_pipeline == pl


def test_paths(tmp_path):
    pl = get_pl(tmp_path)
    pl_path = tmp_path
    assert pl._root_path == pl_path

    for node in pl.nodes:
        assert node._root_path == pl_path / node.name
        assert (
            node._expectations_checkpoint_path
            == pl_path / node.name / "checkpoints" / "expectations"
        )
        for i, s in enumerate(node.all_sinks):
            assert (
                s._checkpoint_path
                == pl_path / node.name / "checkpoints" / f"sink-{s._uuid}"
            )


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_read_write(backend, tmp_path):
    # Build Pipeline
    brz = get_brz(tmp_path, backend)
    slv = get_slv(tmp_path, backend)
    pl = models.Pipeline(name="pl", nodes=[brz, slv], dataframe_backend=backend)

    # Write source data
    ss = StreamingSource(backend)
    df0 = ss.write_to_json(tmp_path / "brz_source")

    # Execute
    pl.execute()
    df = pl.nodes_dict["slv"].primary_sink.read()

    assert_dfs_equal(df, df0)


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_read_write_transform_method(backend, tmp_path):
    # Build Pipeline
    brz = get_brz(tmp_path, backend)
    slv = get_slv(tmp_path, backend)
    slv.transformer = models.DataFrameTransformer(
        nodes=[
            models.DataFrameMethod(
                func_name="with_columns",
                func_kwargs={"y1": "x1"},
            )
        ]
    )
    pl = models.Pipeline(name="pl", nodes=[brz, slv], dataframe_backend=backend)

    # Write source data
    ss = StreamingSource(backend)
    ss.write_to_json(tmp_path / "brz_source")

    # Execute
    pl.execute()
    df = pl.nodes_dict["slv"].primary_sink.read()

    assert sorted(df.columns) == ["_batch_id", "_idx", "id", "x1", "y1"]


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_read_write_transform_expr_df(backend, tmp_path):
    # Build Pipeline
    brz = get_brz(tmp_path, backend)
    slv = get_slv(tmp_path, backend)
    slv.transformer = models.DataFrameTransformer(
        nodes=[
            models.DataFrameExpr(
                expr="SELECT x1 from {df}",
            )
        ]
    )
    pl = models.Pipeline(name="pl", nodes=[brz, slv], dataframe_backend=backend)

    # Write source data
    ss = StreamingSource(backend)
    ss.write_to_json(tmp_path / "brz_source")

    # Execute
    pl.execute()
    df = pl.nodes_dict["slv"].primary_sink.read()

    assert df.columns == ["x1"]


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_read_write_transform_expr_node(backend, tmp_path):
    # Build Pipeline
    brz = get_brz(tmp_path, backend)
    slv = get_slv(tmp_path, backend)
    slv.source = None
    slv.transformer = models.DataFrameTransformer(
        nodes=[
            models.DataFrameExpr(
                expr="SELECT x1 from {nodes.brz}",
            )
        ]
    )
    pl = models.Pipeline(name="pl", nodes=[brz, slv], dataframe_backend=backend)

    # Write source data
    ss = StreamingSource(backend)
    ss.write_to_json(tmp_path / "brz_source")

    # Execute
    pl.execute()
    df = pl.nodes_dict["slv"].primary_sink.read()

    assert df.columns == ["x1"]


#
# def test_execute_node():
#     # Get Pipeline
#     pl, pl_path = get_pl(clean_path=True)
#
#     # Create Stream Source
#     source, source_path = get_source(pl_path)
#
#     # Insert 20 rows and execute each node individually
#     source.filter("index<40").write.format("delta").mode("OVERWRITE").save(source_path)
#     for inode, node in enumerate(pl.sorted_nodes):
#         node.execute(spark=spark)
#         node._output_df = None
#
#     # Insert remaining rows and execute each node individually
#     source.filter("index>=40").write.format("delta").mode("APPEND").save(source_path)
#     for inode, node in enumerate(pl.sorted_nodes):
#         node.execute(spark=spark)
#         node._output_df = None
#
#     # Tests
#     assert pl.nodes_dict["brz_stock_prices"].primary_sink.read(spark).count() == 80
#     assert pl.nodes_dict["slv_stock_prices"].primary_sink.read(spark).count() == 52
#     assert (
#         pl.nodes_dict["slv_stock_prices"].quarantine_sinks[0].read(spark).count() == 8
#     )
#     assert pl.nodes_dict["gld_stock_prices"].primary_sink.read(spark).count() == 3
#     df_gld = pl.nodes_dict["gld_stock_prices"].primary_sink.read(spark).toPandas()
#     assert df_gld.round(0).equals(gld_target)
#
#     # Cleanup
#     shutil.rmtree(pl_path)
