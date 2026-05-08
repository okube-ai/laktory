import narwhals as nw
import polars as pl
import pytest
import yaml

from laktory import models
from laktory.yaml import RecursiveLoader

# --------------------------------------------------------------------------- #
# inject_vars cache (P1 / P2)                                                 #
# --------------------------------------------------------------------------- #


def test_inject_vars_cache_populated():
    node = models.PipelineNode(name="n", comment="${vars.env}")
    assert node._inject_vars_cache_key is None
    node.inject_vars(vars={"env": "dev"})
    assert node._inject_vars_cache_key is not None


def test_inject_vars_cache_hit():
    node = models.PipelineNode(name="n", comment="${vars.env}")
    r1 = node.inject_vars(vars={"env": "dev"})
    r2 = node.inject_vars(vars={"env": "dev"})
    assert r1.comment == r2.comment == "dev"
    assert node._inject_vars_cache_key is not None


# --------------------------------------------------------------------------- #
# Large pipeline inject_vars (P2 / P3)                                        #
# --------------------------------------------------------------------------- #


def test_inject_vars_large_pipeline():
    nodes = [
        models.PipelineNode(name=f"node_{i:03d}", comment="${vars.env}")
        for i in range(100)
    ]
    pipeline = models.Pipeline(name="large-pl", nodes=nodes)
    result = pipeline.inject_vars(vars={"env": "dev"})
    assert len(result.nodes) == 100
    assert all(n.comment == "dev" for n in result.nodes)


# --------------------------------------------------------------------------- #
# YAML large-directory load (P3 — rglob)                                      #
# --------------------------------------------------------------------------- #


def test_yaml_large_directory_load(tmp_path):
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    for i in range(100):
        (config_dir / f"item_{i:03d}.yaml").write_text(f"name: item_{i}\n")

    master = tmp_path / "master.yaml"
    master.write_text(f"items: !use {config_dir}\n")

    with open(master) as f:
        data = yaml.load(f, Loader=RecursiveLoader)

    assert len(data["items"]) == 100


# --------------------------------------------------------------------------- #
# schema_flat depth guard (P4)                                                #
# --------------------------------------------------------------------------- #


def _make_deep_df(depth: int):
    dtype = pl.Int32
    for _ in range(depth):
        dtype = pl.Struct({"child": dtype})
    return nw.from_native(pl.LazyFrame({"root": pl.Series(dtype=dtype)}))


def test_schema_flat_depth_guard():
    df = _make_deep_df(32)
    with pytest.raises(ValueError, match="maximum nesting depth"):
        df.laktory.schema_flat()


def test_schema_flat_within_limit():
    df = _make_deep_df(10)
    fields = df.laktory.schema_flat()
    assert "root" in fields
