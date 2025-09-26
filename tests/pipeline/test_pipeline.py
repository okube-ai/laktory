import io
from pathlib import Path

import networkx as nx
import polars
import pytest

from laktory import get_spark_session
from laktory import models
from laktory._testing import StreamingSource
from laktory._version import VERSION

from ..conftest import assert_dfs_equal

data_dirpath = Path(__file__).parent.parent / "data"

OPEN_FIGURES = False


def get_pl(tmp_path):
    with open(data_dirpath / "pl.yaml") as fp:
        data = fp.read()
        data = data.replace("{tmp_path}", str(tmp_path))
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))
        pl.root_path_ = tmp_path

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
        job_clusters=[
            {
                "job_cluster_key": "node-cluster",
                "new_cluster": {
                    "node_type_id": "Standard_DS3_v2",
                    "spark_version": "16.3.x-scala2.12",
                },
            }
        ],
    )

    # Assign As Dict
    pl = get_pl("")
    print(o.model_dump(exclude_unset=True))
    pl.orchestrator = o.model_dump(exclude_unset=True)

    # Test
    assert pl.orchestrator.parent == pl
    assert pl.orchestrator.parent_pipeline == pl
    assert len(pl.orchestrator.parameters) == 1

    # Assign As Model
    pl = get_pl("")
    pl.orchestrator = o

    # Test
    assert pl.orchestrator.parent == pl
    assert pl.orchestrator.parent_pipeline == pl
    assert len(pl.orchestrator.parameters) == 1


def test_root_path(tmp_path):
    # Without Path
    pl = models.Pipeline(name="pl")
    dump = pl.model_dump(exclude_unset=True)
    dumpj = pl.model_dump(exclude_unset=True, mode="json")
    pl2 = models.Pipeline.model_validate(dump)
    pl3 = models.Pipeline.model_validate(dumpj)

    assert pl.root_path_ is None
    assert pl.root_path == Path("pipelines/pl")
    assert list(dump.keys()) == [
        "name",
        "dataframe_backend",
        "dataframe_api",
        "root_path",
    ]
    assert dump["root_path"] == Path("pipelines/pl")
    assert dumpj["root_path"] == "pipelines/pl"
    assert pl2.root_path_ == Path("pipelines/pl")
    assert pl3.root_path_ == "pipelines/pl"

    # With Path
    pl = models.Pipeline(name="pl", root_path="/pl_root/")
    dump = pl.model_dump(exclude_unset=True)
    pl2 = models.Pipeline.model_validate(dump)

    assert pl.root_path_ == "/pl_root/"
    assert pl.root_path == Path("/pl_root/")
    assert list(dump.keys()) == [
        "name",
        "dataframe_backend",
        "dataframe_api",
        "root_path",
    ]
    assert dump["root_path"] == Path("/pl_root/")
    assert pl2.root_path_ == Path("/pl_root/")


def test_paths(tmp_path):
    pl = get_pl(tmp_path)
    pl_path = tmp_path
    assert pl.root_path == pl_path

    for node in pl.nodes:
        assert node.root_path == pl_path / node.name
        assert (
            node.expectations_checkpoint_path
            == pl_path / node.name / "checkpoints" / "expectations"
        )
        for i, s in enumerate(node.all_sinks):
            assert (
                s.checkpoint_path
                == pl_path / node.name / "checkpoints" / f"sink-{s._uuid}"
            )


def test_dependencies(tmp_path):
    pl = get_pl(tmp_path)
    assert pl._dependencies == [
        "requests>=2.0",
        "./wheels/lake-0.0.1-py3-none-any.whl",
        f"laktory=={VERSION}",
    ]
    assert pl._imports == ["re", "requests", "lake"]


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
    pl.dataframe_backend_ = backend

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


@pytest.mark.parametrize("backend", ["PYSPARK"])
def test_update_metadata(backend, tmp_path):
    spark = get_spark_session()

    pl = get_pl(tmp_path)
    pl.dataframe_backend_ = backend

    # Set Source
    ss = StreamingSource(backend)

    # Execute
    ss.write_to_json(tmp_path / "brz_source")
    pl.execute(update_tables_metadata=False)

    meta = (
        spark.sql("DESCRIBE TABLE EXTENDED default.gld")
        .toPandas()
        .set_index("col_name")
    )
    dtypes = meta["data_type"].to_dict()
    comments = meta["comment"].to_dict()
    assert comments["id"] is None
    assert dtypes.get("Comment", None) is None

    # Update metadata
    pl.update_tables_metadata()

    meta = (
        spark.sql("DESCRIBE TABLE EXTENDED default.gld")
        .toPandas()
        .set_index("col_name")
    )
    dtypes = meta["data_type"].to_dict()
    comments = meta["comment"].to_dict()
    print(meta.to_string())
    dtypes = meta["data_type"].to_dict()
    assert comments["id"] == "Identification column"
    assert dtypes.get("Comment", None) == "Gold"
