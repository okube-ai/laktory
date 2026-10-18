"""Tests for pipeline parameters (`${params.<name>}`), resolved at run time (#694)."""

import json

import polars
import pytest
from pydantic import ValidationError

from laktory import models


def _get_pl(**kwargs):
    kwargs = {"root_path": "/Volumes/dev/laktory/pipelines/pl-sales", **kwargs}
    return models.Pipeline(
        name="pl-sales",
        parameters={"client": None, "region": {"default": "eu"}},
        nodes=[
            {
                "name": "slv_sales",
                "source": {
                    "catalog_name": "dev",
                    "schema_name": "bronze",
                    "table_name": "brz_sales",
                    "as_stream": True,
                },
                "sinks": [
                    {
                        "catalog_name": "dev",
                        "schema_name": "${params.client}_silver",
                        "table_name": "slv_sales_${params.region}",
                    }
                ],
            }
        ],
        **kwargs,
    )


def test_resolve_parameters():
    pl = _get_pl()

    pl_acme = pl.resolve_parameters({"client": "acme"})
    assert pl_acme.nodes[0].sinks[0].full_name == "dev.acme_silver.slv_sales_eu"
    assert pl_acme.nodes[0].sinks[0].parent_pipeline is pl_acme

    # Empty values (e.g. job parameters not set) fall back to the default
    pl_globex = pl.resolve_parameters({"client": "globex", "region": ""})
    assert pl_globex.nodes[0].sinks[0].full_name == "dev.globex_silver.slv_sales_eu"

    # Original pipeline is not modified
    assert (
        pl.nodes[0].sinks[0].full_name
        == "dev.${params.client}_silver.slv_sales_${params.region}"
    )


def test_resolve_parameters_errors():
    pl = _get_pl()

    with pytest.raises(ValueError, match=r"Parameters \['client'\] .* are required"):
        pl.resolve_parameters({})
    with pytest.raises(ValueError, match=r"Parameters \['client'\] .* are required"):
        pl.resolve_parameters({"client": ""})
    with pytest.raises(ValueError, match=r"Parameters \['x'\] are not declared"):
        pl.resolve_parameters({"client": "acme", "x": "1"})


def test_checkpoint_per_parameter_value():
    pl = _get_pl(
        orchestrator={"type": "LAKEFLOW_JOB", "serverless_environment_version": "4"}
    )

    # Deployed config: the checkpoint is derived at run time, once resolved
    config = pl.orchestrator.config_file.content_dict
    assert config["nodes"][0]["sinks"][0]["checkpoint_path"] is None
    assert config["parameters"] == {"client": {}, "region": {"default": "eu"}}

    pl = models.Pipeline.model_validate_json(json.dumps(config))
    paths = {
        c: pl.resolve_parameters({"client": c}).nodes[0].sinks[0].checkpoint_path
        for c in ["acme", "globex"]
    }
    assert paths["acme"] != paths["globex"]
    assert all(str(p).startswith(str(pl.nodes[0].root_path)) for p in paths.values())


def test_explicit_checkpoint_kept():
    pl = _get_pl()
    pl.nodes[0].sinks[0].checkpoint_path_ = "/checkpoints/${params.client}"
    dump = pl.model_dump(mode="json")
    assert (
        dump["nodes"][0]["sinks"][0]["checkpoint_path"]
        == "/checkpoints/${params.client}"
    )
    pl_acme = pl.resolve_parameters({"client": "acme"})
    assert pl_acme.nodes[0].sinks[0].checkpoint_path.as_posix() == "/checkpoints/acme"


def test_validation():
    # Reserved name
    with pytest.raises(ValidationError, match="Parameter name 'refresh' is reserved"):
        models.Pipeline(name="pl", parameters={"refresh": {}})

    # Undeclared parameter
    with pytest.raises(
        ValidationError, match=r"Parameters \['region'\] .* not declared"
    ):
        models.Pipeline(
            name="pl",
            parameters={"client": {}},
            nodes=[
                {
                    "name": "n",
                    "sinks": [{"path": "/${params.region}/n", "format": "PARQUET"}],
                }
            ],
        )

    # Used outside of the nodes
    with pytest.raises(ValidationError, match=r"used outside of the pipeline nodes"):
        _get_pl(
            orchestrator={
                "type": "LAKEFLOW_JOB",
                "name": "job-${params.client}",
                "serverless_environment_version": "4",
            }
        )

    # Used in a node name
    with pytest.raises(ValidationError, match=r"use parameters in their name"):
        models.Pipeline(
            name="pl",
            parameters={"client": {}},
            nodes=[{"name": "brz_${params.client}", "source": {"table_name": "x"}}],
        )

    # Run time root path is supported
    pl = _get_pl(root_path="/Volumes/dev/laktory/${params.client}")
    pl = pl.resolve_parameters({"client": "acme"})
    assert pl.nodes[0].root_path.as_posix() == "/Volumes/dev/laktory/acme/slv_sales"

    # Declarative pipelines
    with pytest.raises(ValidationError, match="not supported with"):
        _get_pl(orchestrator={"type": "LAKEFLOW_DECLARATIVE_PIPELINE"})


def test_execute(tmp_path):
    pl = models.Pipeline(
        name="pl",
        dataframe_backend="POLARS",
        parameters={"client": {}},
        nodes=[
            {
                "name": "brz",
                "source": {"data": {"client": ["${params.client}"], "x": [1]}},
                "sinks": [
                    {
                        "format": "PARQUET",
                        "path": f"{tmp_path}/${{params.client}}.parquet",
                    }
                ],
            }
        ],
    )

    pl.execute(params={"client": "acme"})
    pl.execute(params={"client": "globex"})

    for client in ["acme", "globex"]:
        df = polars.read_parquet(tmp_path / f"{client}.parquet")
        assert df.to_dicts() == [{"client": client, "x": 1}]

    with pytest.raises(ValueError, match="are required"):
        pl.execute()


def test_execute_undeclared_parameters():
    pl = models.Pipeline(name="pl", nodes=[{"name": "n"}])
    with pytest.raises(ValueError, match=r"Parameters \['client'\] are not declared"):
        pl.execute(params={"client": "acme"})
