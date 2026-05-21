import time

import narwhals as nw
import polars as pl
import pytest
import yaml

from laktory import models
from laktory.yaml import RecursiveLoader

# Future improvement: replace time.perf_counter() thresholds with
# pytest-benchmark (https://pytest-benchmark.readthedocs.io). It auto-calibrates
# iteration counts and compares against a saved baseline, catching gradual creep
# rather than only catastrophic regressions. Deferred because it requires
# committing a baseline file and updating it after intentional speedups.

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
# Large stack inject_vars (many pipelines × many nodes)                       #
# --------------------------------------------------------------------------- #


def test_inject_vars_large_stack():
    # 10 variables so each lookup scans a realistic-sized dict
    variables = {f"var_{k}": f"value_{k}" for k in range(10)}

    n_pipelines, n_nodes = 10, 100
    stack = models.Stack(
        name="perf-stack",
        organization="test",
        resources={
            "pipelines": {
                f"pl-{j:03d}": models.Pipeline(
                    name=f"pl-{j:03d}",
                    nodes=[
                        models.PipelineNode(
                            name=f"node_{i:03d}",
                            # 4 variable references per node across 2 fields
                            comment="${vars.var_0}/${vars.var_1}/${vars.var_2}",
                            ldp_template="${vars.var_3}",
                        )
                        for i in range(n_nodes)
                    ],
                )
                for j in range(n_pipelines)
            }
        },
        variables=variables,
        environments={"dev": {}},
    )
    t0 = time.perf_counter()
    result = stack.get_env("dev").inject_vars()
    assert time.perf_counter() - t0 < 1.0  # actual ~0.13 s; generous for slow CI

    pls = result.resources.pipelines
    assert len(pls) == n_pipelines
    assert all(len(pl.nodes) == n_nodes for pl in pls.values())
    assert all(
        n.comment == "value_0/value_1/value_2" for pl in pls.values() for n in pl.nodes
    )


# --------------------------------------------------------------------------- #
# inject_vars cache speedup (P1 / P2)                                         #
# --------------------------------------------------------------------------- #


def test_inject_vars_cache_speedup():
    nodes = [
        models.PipelineNode(
            name=f"node_{i:03d}",
            comment="${vars.var_0}/${vars.var_1}/${vars.var_2}",
            ldp_template="${vars.var_3}",
        )
        for i in range(100)
    ]
    pipeline = models.Pipeline(name="large-pl", nodes=nodes)
    n = 20

    vars_fixed = {f"var_{k}": f"value_{k}" for k in range(10)}
    pipeline.inject_vars(vars=vars_fixed)  # warm — populates cache

    t0 = time.perf_counter()
    for _ in range(n):
        pipeline.inject_vars(vars=vars_fixed)  # cache hits
    cached_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    for i in range(n):
        pipeline.inject_vars(vars={**vars_fixed, "var_0": f"miss_{i}"})  # cache misses
    uncached_time = time.perf_counter() - t0

    assert cached_time < uncached_time


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


# --------------------------------------------------------------------------- #
# Terraform ref substitution at scale (A4 regression guard)                  #
# --------------------------------------------------------------------------- #


def test_terraform_ref_substitution_scale():
    """
    _substitute_terraform_refs must scale O(N), not O(N×M).

    Regression guard for the bug introduced in 34daee53 (Code cleanup, between
    v0.11.4 and v0.11.6) which replaced the single-pass JSON-string substitution
    with a per-string loop over all resource patterns, degrading from O(N+M) to
    O(N×M) for N string values and M resources.

    300 resources × 5 fields = 1500 strings should complete in < 0.5 s with the
    single combined-regex approach; the old O(N×M) implementation took ~3-5 s.
    """
    import re

    from laktory.models.stacks.terraformstack import _substitute_terraform_refs

    n = 300

    class _MockResource:
        def __init__(self, name):
            self.resource_name = name
            self.terraform_resource_type = "databricks_catalog"
            self.lookup_existing = None

    resource_lookup = {f"res_{i:03d}": _MockResource(f"res_{i:03d}") for i in range(n)}

    names_alt = "|".join(re.escape(k) for k in resource_lookup)
    pattern = re.compile(r"\$\{resources\.(" + names_alt + r")(?:\.([^}]*))?\}")

    def replacer(m, _lookup=resource_lookup):
        name, prop = m.group(1), m.group(2)
        r = _lookup[name]
        base = f"{r.terraform_resource_type}.{name}"
        return f"${{{base}.{prop}}}" if prop is not None else base

    # 300 resources × 5 fields (2 refs + 3 literals) = 1500 strings
    d = {
        f"res_{i:03d}": {
            "depends_on": [f"${{resources.res_{(i - 1) % n:03d}}}"],
            "catalog": "${resources.res_000.name}",
            "no_ref_a": "some-literal-value",
            "no_ref_b": "abcdefghijklmnopqrstuvwxyz",
            "no_ref_c": f"static-{i}",
        }
        for i in range(n)
    }

    t0 = time.perf_counter()
    result = _substitute_terraform_refs(d, pattern, replacer)
    elapsed = time.perf_counter() - t0

    assert elapsed < 0.5, (
        f"_substitute_terraform_refs took {elapsed:.2f}s for {n} resources "
        f"(threshold 0.5s — O(N×M) regression?)"
    )

    # Correctness spot-checks
    assert result["res_001"]["depends_on"] == ["databricks_catalog.res_000"]
    assert result["res_001"]["catalog"] == "${databricks_catalog.res_000.name}"
    assert result["res_001"]["no_ref_a"] == "some-literal-value"
    assert result["res_000"]["depends_on"] == [f"databricks_catalog.res_{n - 1:03d}"]
