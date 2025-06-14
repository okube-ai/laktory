import io
from pathlib import Path

import networkx as nx
import polars
import pytest

from laktory import get_spark_session
from laktory import models
from laktory._testing import StreamingSource
from laktory._testing import assert_dfs_equal

data_dirpath = Path(__file__).parent.parent / "data"

OPEN_FIGURES = False


def get_pl(tmp_path):
    with open(data_dirpath / "pl.yaml") as fp:
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


def test_dag(tmp_path):
    pl = get_pl(tmp_path)

    dag = pl.dag

    # Test Dag
    assert nx.is_directed_acyclic_graph(dag)
    assert len(dag.nodes) == 6
    assert len(dag.edges) == 6
    assert list(nx.topological_sort(dag)) == [
        "brz",
        "slv",
        "gld",
        "gld_a",
        "gld_b",
        "gld_ab",
    ]

    # Test nodes assignment
    assert [n.name for n in pl.sorted_nodes] == [
        "brz",
        "slv",
        "gld",
        "gld_a",
        "gld_b",
        "gld_ab",
    ]

    # Test figure
    fig = pl.dag_figure()
    fig.write_html(tmp_path / "dag.html", auto_open=OPEN_FIGURES)


def test_children(tmp_path):
    pl = get_pl(tmp_path)

    for pn in pl.nodes:
        assert pn.parent == pl
        assert pn.parent_pipeline == pl
        if pn.source:
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


def test_update_from_parent():
    o = models.DatabricksJobOrchestrator(
        name="pl-job",
        clusters=[
            {
                "name": "node-cluster",
                "node_type_id": "Standard_DS3_v2",
                "spark_version": "16.3.x-scala2.12",
            }
        ],
    )

    # Assign As Dict
    pl = get_pl("")
    pl.orchestrator = o.model_dump(exclude_unset=True)

    # Test
    assert pl.orchestrator.parent == pl
    assert pl.orchestrator.parent_pipeline == pl
    assert len(pl.orchestrator.parameters) == 5
    assert pl.orchestrator.config_file.content_base64.startswith(
        "ewogICAgIm5hbWUiOiAicGwtbG9j"
    )

    # Assign As Model
    pl = get_pl("")
    pl.orchestrator = o

    # Test
    assert pl.orchestrator.parent == pl
    assert pl.orchestrator.parent_pipeline == pl
    assert len(pl.orchestrator.parameters) == 5
    assert pl.orchestrator.config_file.content_base64.startswith(
        "ewogICAgIm5hbWUiOiAicGwtbG9j"
    )


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


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_read_write_view(backend, tmp_path):
    if backend not in ["PYSPARK"]:
        pytest.skip(f"Backend '{backend}' not implemented.")

    # Build Pipeline
    brz = get_brz(tmp_path, backend)
    brz.sinks = [
        models.HiveMetastoreDataSink(
            schema_name="default",
            table_name="brz",
            format="PARQUET",
            mode="OVERWRITE",
            writer_kwargs={"path": f"{tmp_path}/gld_sink/"},
        )
    ]
    slv = get_slv(tmp_path, backend)
    slv.source = None
    slv.sinks = [
        models.HiveMetastoreDataSink(
            schema_name="default",
            table_name="slv",
            table_type="VIEW",
            view_definition="SELECT * from {nodes.brz}",
        )
    ]
    pl = models.Pipeline(name="pl", nodes=[brz, slv], dataframe_backend=backend)

    # Write source data
    ss = StreamingSource(backend)
    df0 = ss.write_to_json(tmp_path / "brz_source")

    # Execute
    pl.execute()
    df = pl.nodes_dict["slv"].primary_sink.read()

    # Test
    assert_dfs_equal(df, df0)


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_single_node(backend, tmp_path):
    # Build Pipeline
    brz = get_brz(tmp_path, backend)
    slv = get_slv(tmp_path, backend)
    pl = models.Pipeline(name="pl", nodes=[brz, slv], dataframe_backend=backend)

    # Write source data
    ss = StreamingSource(backend)
    df0 = ss.write_to_json(tmp_path / "brz_source")

    # Execute individually
    for inode, node in enumerate(pl.sorted_nodes):
        node.execute()
        node._output_df = None

    df = pl.nodes_dict["slv"].primary_sink.read()

    assert_dfs_equal(df, df0)


@pytest.mark.parametrize("backend", ["PYSPARK"])
def test_full(backend, tmp_path):
    pl = get_pl(tmp_path)
    pl.dataframe_backend = backend

    # Set Source
    ss = StreamingSource(backend)

    # Execute
    ss.write_to_json(tmp_path / "brz_source")
    pl.execute()

    # Execute Again
    ss.write_to_json(tmp_path / "brz_source")
    pl.execute()

    # Test
    df = get_spark_session().read.table("default.gld_ab")
    assert_dfs_equal(df, polars.DataFrame({"id": ["a", "b"], "max_x1": [1, 2]}))
