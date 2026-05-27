"""End-to-end Pipeline execution tests."""

import pytest

from laktory import models
from laktory._testing import StreamingSource

from ...conftest import assert_dfs_equal


def _file_sink(path, fmt="PARQUET", mode=None):
    return {"format": fmt, "path": str(path), "mode": mode}


def _build_2node_pl(tmp_path, backend):
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    brz_path = str(tmp_path / "brz_sink") + ("/" if backend == "PYSPARK" else "")
    slv_path = str(tmp_path / "slv_sink") + ("/" if backend == "PYSPARK" else "")
    # Polars reads a specific JSON file; PySpark reads a directory
    src_path = (
        str(tmp_path / "brz_source" / "000.json")
        if backend == "POLARS"
        else str(tmp_path / "brz_source") + "/"
    )

    return models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(
                name="brz",
                source={"format": "JSON", "path": src_path},
                sinks=[_file_sink(brz_path, mode=mode)],
            ),
            models.PipelineNode(
                name="slv",
                source={"node_name": "brz"},
                sinks=[_file_sink(slv_path, mode=mode)],
            ),
        ],
        dataframe_backend=backend,
    )


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_linear_3node(backend, tmp_path):
    ss = StreamingSource(backend)
    df0 = ss.write_to_json(tmp_path / "brz_source")

    pl = _build_2node_pl(tmp_path, backend)
    pl.execute()
    df = pl.nodes_dict["slv"].primary_sink.read()

    assert_dfs_equal(df, df0)


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_mixed_write_modes(backend, tmp_path):
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    brz_path = str(tmp_path / "brz_sink") + ("/" if backend == "PYSPARK" else "")
    src_path = (
        str(tmp_path / "brz_source" / "000.json")
        if backend == "POLARS"
        else str(tmp_path / "brz_source") + "/"
    )

    ss = StreamingSource(backend)
    ss.write_to_json(tmp_path / "brz_source")

    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(
                name="brz",
                source={"format": "JSON", "path": src_path},
                transformer={
                    "nodes": [
                        {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}}
                    ]
                },
                sinks=[_file_sink(brz_path, mode=mode)],
            ),
        ],
        dataframe_backend=backend,
    )
    pl.execute()
    df = pl.nodes_dict["brz"].primary_sink.read()
    assert sorted(df.columns) == ["_batch_id", "_idx", "id", "x1", "y1"]


def test_streaming_e2e(tmp_path):
    ss = StreamingSource("PYSPARK")
    ss.write_to_delta(str(tmp_path / "source"))

    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(
                name="brz",
                source={
                    "format": "DELTA",
                    "path": str(tmp_path / "source"),
                    "as_stream": True,
                },
                sinks=[
                    {
                        "format": "DELTA",
                        "path": str(tmp_path / "sink"),
                        "mode": "APPEND",
                    }
                ],
            ),
        ],
        dataframe_backend="PYSPARK",
    )
    pl.execute()
    df = pl.nodes_dict["brz"].primary_sink.read()
    assert df.collect().shape[0] == 3


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_partial_selects(backend, tmp_path):
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    brz_path = str(tmp_path / "brz_sink") + ("/" if backend == "PYSPARK" else "")
    slv_path = str(tmp_path / "slv_sink") + ("/" if backend == "PYSPARK" else "")
    src_path = (
        str(tmp_path / "brz_source" / "000.json")
        if backend == "POLARS"
        else str(tmp_path / "brz_source") + "/"
    )

    ss = StreamingSource(backend)
    ss.write_to_json(tmp_path / "brz_source")

    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(
                name="brz",
                source={"format": "JSON", "path": src_path},
                sinks=[_file_sink(brz_path, mode=mode)],
            ),
            models.PipelineNode(
                name="slv",
                source={"node_name": "brz"},
                sinks=[_file_sink(slv_path, mode=mode)],
            ),
        ],
        dataframe_backend=backend,
    )

    # Execute only brz
    pl.execute(selects=["brz"])
    assert pl.nodes_dict["brz"].output_df is not None
    assert pl.nodes_dict["slv"].output_df is None


@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_full_refresh(backend, tmp_path):
    mode = "OVERWRITE" if backend == "PYSPARK" else None
    brz_path = str(tmp_path / "brz_sink") + ("/" if backend == "PYSPARK" else "")
    src_path = (
        str(tmp_path / "brz_source" / "000.json")
        if backend == "POLARS"
        else str(tmp_path / "brz_source") + "/"
    )

    ss = StreamingSource(backend)
    ss.write_to_json(tmp_path / "brz_source")

    pl = models.Pipeline(
        name="pl",
        nodes=[
            models.PipelineNode(
                name="brz",
                source={"format": "JSON", "path": src_path},
                sinks=[_file_sink(brz_path, mode=mode)],
            ),
        ],
        dataframe_backend=backend,
    )

    pl.execute()
    pl.execute(full_refresh=True)  # should not raise
    df = pl.nodes_dict["brz"].primary_sink.read()
    assert df.collect().shape[0] == 3
