import io
import json
from pathlib import Path

from laktory import __version__
from laktory import models
from laktory.enums import DataFrameBackends

data_dirpath = Path(__file__).parent.parent / "data"
testdir_path = Path(__file__).parent


def get_pl(tmp_path="", is_dlt=False):
    filepath = data_dirpath / "pl.yaml"
    if is_dlt:
        filepath = data_dirpath / "pl_dlt.yaml"

    with open(filepath) as fp:
        data = fp.read()
        data = data.replace("{tmp_path}", str(tmp_path))
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))

        pl.root_path = tmp_path

    return pl


# Job
def get_pl_job():
    pl = get_pl()
    pl.name = "pl-job"
    pl.dependencies = ["yfinance"]
    pl.orchestrator = {
        "clusters": [
            {
                "name": "node-cluster",
                "node_type_id": "Standard_DS3_v2",
                "spark_version": "16.3.x-scala2.12",
            }
        ],
        "name": "pl-job",
        "type": "DATABRICKS_JOB",
    }

    return pl


# DLT
def get_pl_dlt():
    pl = get_pl(is_dlt=True)
    return pl


def test_pipeline_job():
    # Test job
    job = get_pl_job().orchestrator
    data = job.model_dump(exclude_unset=True)
    data = json.loads(
        json.dumps(data).replace(f"laktory=={__version__}", "laktory==__version__")
    )
    print(data)
    assert data == {
        "clusters": [
            {
                "name": "node-cluster",
                "node_type_id": "Standard_DS3_v2",
                "spark_version": "16.3.x-scala2.12",
            }
        ],
        "name": "pl-job",
        "parameters": [
            {"default": "false", "name": "full_refresh"},
            {"default": "pl-job", "name": "pipeline_name"},
            {"default": "false", "name": "install_dependencies"},
            {"default": '["yfinance", "laktory==__version__"]', "name": "requirements"},
            {
                "default": "/Workspace/.laktory/pipelines/pl-job/config.json",
                "name": "config_filepath",
            },
        ],
        "tasks": [
            {
                "depends_ons": [],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "yfinance"}},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "brz"},
                },
                "task_key": "node-brz",
            },
            {
                "depends_ons": [{"task_key": "node-slv"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "yfinance"}},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "gld"},
                },
                "task_key": "node-gld",
            },
            {
                "depends_ons": [{"task_key": "node-gld"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "yfinance"}},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "gld_a"},
                },
                "task_key": "node-gld_a",
            },
            {
                "depends_ons": [{"task_key": "node-gld_a"}, {"task_key": "node-gld_b"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "yfinance"}},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "gld_ab"},
                },
                "task_key": "node-gld_ab",
            },
            {
                "depends_ons": [{"task_key": "node-gld"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "yfinance"}},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "gld_b"},
                },
                "task_key": "node-gld_b",
            },
            {
                "depends_ons": [{"task_key": "node-brz"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "yfinance"}},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "slv"},
                },
                "task_key": "node-slv",
            },
        ],
        "type": "DATABRICKS_JOB",
    }

    # Test resources
    resources = job.core_resources
    assert len(resources) == 3


def test_pipeline_dlt(tmp_path):
    pl = get_pl_dlt()

    # Test node names
    assert pl.nodes_dict["brz"].primary_sink.dlt_name == "dev.sandbox.brz"
    assert pl.nodes_dict["slv"].primary_sink.dlt_name == "dev.sandbox.slv"
    assert pl.nodes_dict["gld"].primary_sink.dlt_name == "gld"
    assert pl.nodes_dict["gld_a"].primary_sink.dlt_name == "dev.sandbox2.gld_a"
    assert pl.nodes_dict["gld_b"].primary_sink.dlt_name == "dev.sandbox2.gld_b"
    assert pl.nodes_dict["gld_ab"].primary_sink.dlt_name == "prd.sandbox2.gld_ab"

    # Test Sink as Source
    node_slv = pl.nodes_dict["slv"]
    sink_source = node_slv.source.node.primary_sink.as_source(
        as_stream=node_slv.source.as_stream
    )
    data = sink_source.model_dump()
    assert data.pop("dataframe_backend") == DataFrameBackends.PYSPARK
    print(data)
    assert data == {
        "dataframe_api": None,
        "as_stream": False,
        "drop_duplicates": None,
        "drops": None,
        "filter": None,
        "renames": None,
        "selects": None,
        "type": "UNITY_CATALOG",
        "catalog_name": "dev",
        "schema_name": "sandbox",
        "table_name": "brz",
        "reader_methods": [],
    }

    data = pl.orchestrator.model_dump()
    print(data)
    assert data == {
        "dataframe_backend": None,
        "dataframe_api": None,
        "access_controls": [
            {
                "group_name": "account users",
                "permission_level": "CAN_VIEW",
                "service_principal_name": None,
                "user_name": None,
            }
        ],
        "allow_duplicate_names": None,
        "budget_policy_id": None,
        "catalog": "dev",
        "cause": None,
        "channel": "PREVIEW",
        "cluster_id": None,
        "clusters": [],
        "creator_user_name": None,
        "configuration": {
            "pipeline_name": "pl-dlt",
            "requirements": '["laktory==0.8.0"]',
            "config_filepath": "/Workspace/.laktory/pipelines/pl-dlt/config.json",
        },
        "continuous": None,
        "deployment": None,
        "development": None,
        "edition": None,
        "event_log": None,
        "expected_last_modified": None,
        "filters": None,
        "gateway_definition": None,
        "health": None,
        "last_modified": None,
        "latest_updates": None,
        "libraries": None,
        "name": "pl-dlt",
        "name_prefix": None,
        "name_suffix": None,
        "notifications": [],
        "photon": None,
        "restart_window": None,
        "root_path": None,
        "run_as": None,
        "run_as_user_name": None,
        "schema_": "sandbox",
        "serverless": None,
        "state": None,
        "storage": None,
        "target": None,
        "trigger": None,
        "url": None,
        "type": "DATABRICKS_DLT",
        "config_file": {
            "dataframe_backend": None,
            "dataframe_api": None,
            "access_controls": [
                {
                    "group_name": "users",
                    "permission_level": "CAN_READ",
                    "service_principal_name": None,
                    "user_name": None,
                }
            ],
            "dirpath": None,
            "path": "/.laktory/pipelines/pl-dlt/config.json",
            "rootpath": None,
            "source": None,
            "content_base64": "ewogICAgIm5hbWUiOiAicGwtZGx0IiwKICAgICJub2RlcyI6IFsKICAgICAgICB7CiAgICAgICAgICAgICJuYW1lIjogImdsZF9hYiIsCiAgICAgICAgICAgICJzaW5rcyI6IFsKICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAiY2F0YWxvZ19uYW1lIjogInByZCIsCiAgICAgICAgICAgICAgICAgICAgInNjaGVtYV9uYW1lIjogInNhbmRib3gyIiwKICAgICAgICAgICAgICAgICAgICAidGFibGVfbmFtZSI6ICJnbGRfYWIiCiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgIF0sCiAgICAgICAgICAgICJ0cmFuc2Zvcm1lciI6IHsKICAgICAgICAgICAgICAgICJub2RlcyI6IFsKICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgICAgICJleHByIjogIlNFTEVDVCAqIGZyb20ge25vZGVzLmdsZF9hfSBVTklPTiBTRUxFQ1QgKiBmcm9tIHtub2Rlcy5nbGRfYn0iCiAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgXQogICAgICAgICAgICB9CiAgICAgICAgfSwKICAgICAgICB7CiAgICAgICAgICAgICJuYW1lIjogImJyeiIsCiAgICAgICAgICAgICJzb3VyY2UiOiB7CiAgICAgICAgICAgICAgICAiZm9ybWF0IjogIkpTT04iLAogICAgICAgICAgICAgICAgInBhdGgiOiAiL2Jyel9zb3VyY2UvIgogICAgICAgICAgICB9LAogICAgICAgICAgICAic2lua3MiOiBbCiAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgImNhdGFsb2dfbmFtZSI6ICJkZXYiLAogICAgICAgICAgICAgICAgICAgICJzY2hlbWFfbmFtZSI6ICJzYW5kYm94IiwKICAgICAgICAgICAgICAgICAgICAidGFibGVfbmFtZSI6ICJicnoiCiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgIF0KICAgICAgICB9LAogICAgICAgIHsKICAgICAgICAgICAgIm5hbWUiOiAic2x2IiwKICAgICAgICAgICAgInNvdXJjZSI6IHsKICAgICAgICAgICAgICAgICJub2RlX25hbWUiOiAiYnJ6IgogICAgICAgICAgICB9LAogICAgICAgICAgICAic2lua3MiOiBbCiAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgImNhdGFsb2dfbmFtZSI6ICJkZXYiLAogICAgICAgICAgICAgICAgICAgICJzY2hlbWFfbmFtZSI6ICJzYW5kYm94IiwKICAgICAgICAgICAgICAgICAgICAidGFibGVfbmFtZSI6ICJzbHYiCiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgIF0sCiAgICAgICAgICAgICJ0cmFuc2Zvcm1lciI6IHsKICAgICAgICAgICAgICAgICJub2RlcyI6IFsKICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgICAgICJmdW5jX2t3YXJncyI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ5MSI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidmFsdWUiOiAieDEiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgICAgICJmdW5jX25hbWUiOiAid2l0aF9jb2x1bW5zIgogICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAiZXhwciI6ICJTRUxFQ1QgaWQsIHgxLCB5MSBmcm9tIHtkZn0iCiAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgXQogICAgICAgICAgICB9CiAgICAgICAgfSwKICAgICAgICB7CiAgICAgICAgICAgICJuYW1lIjogImdsZCIsCiAgICAgICAgICAgICJzaW5rcyI6IFsKICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAiZGx0X3ZpZXdfbmFtZSI6ICJnbGQiCiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgIF0sCiAgICAgICAgICAgICJ0cmFuc2Zvcm1lciI6IHsKICAgICAgICAgICAgICAgICJub2RlcyI6IFsKICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgICAgICJleHByIjogIlNFTEVDVCBpZCwgTUFYKHgxKSBBUyBtYXhfeDEgZnJvbSB7bm9kZXMuc2x2fSBHUk9VUCBCWSBpZCIKICAgICAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICAgICBdCiAgICAgICAgICAgIH0KICAgICAgICB9LAogICAgICAgIHsKICAgICAgICAgICAgIm5hbWUiOiAiZ2xkX2EiLAogICAgICAgICAgICAic2lua3MiOiBbCiAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgImNhdGFsb2dfbmFtZSI6ICJkZXYiLAogICAgICAgICAgICAgICAgICAgICJzY2hlbWFfbmFtZSI6ICJzYW5kYm94MiIsCiAgICAgICAgICAgICAgICAgICAgInRhYmxlX25hbWUiOiAiZ2xkX2EiCiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgIF0sCiAgICAgICAgICAgICJ0cmFuc2Zvcm1lciI6IHsKICAgICAgICAgICAgICAgICJub2RlcyI6IFsKICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgICAgICJleHByIjogIlNFTEVDVCAqIGZyb20ge25vZGVzLmdsZH0gV0hFUkUgaWQgPSAnYSciCiAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgXQogICAgICAgICAgICB9CiAgICAgICAgfSwKICAgICAgICB7CiAgICAgICAgICAgICJuYW1lIjogImdsZF9iIiwKICAgICAgICAgICAgInNpbmtzIjogWwogICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICJjYXRhbG9nX25hbWUiOiAiZGV2IiwKICAgICAgICAgICAgICAgICAgICAic2NoZW1hX25hbWUiOiAic2FuZGJveDIiLAogICAgICAgICAgICAgICAgICAgICJ0YWJsZV9uYW1lIjogImdsZF9iIgogICAgICAgICAgICAgICAgfQogICAgICAgICAgICBdLAogICAgICAgICAgICAidHJhbnNmb3JtZXIiOiB7CiAgICAgICAgICAgICAgICAibm9kZXMiOiBbCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAiZXhwciI6ICJTRUxFQ1QgKiBmcm9tIHtub2Rlcy5nbGR9IFdIRVJFIGlkID0gJ2InIgogICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgIF0KICAgICAgICAgICAgfQogICAgICAgIH0KICAgIF0sCiAgICAicm9vdF9wYXRoIjogIiIsCiAgICAib3JjaGVzdHJhdG9yIjogewogICAgICAgICJhY2Nlc3NfY29udHJvbHMiOiBbCiAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICJncm91cF9uYW1lIjogImFjY291bnQgdXNlcnMiLAogICAgICAgICAgICAgICAgInBlcm1pc3Npb25fbGV2ZWwiOiAiQ0FOX1ZJRVciCiAgICAgICAgICAgIH0KICAgICAgICBdLAogICAgICAgICJjYXRhbG9nIjogImRldiIsCiAgICAgICAgIm5hbWUiOiAicGwtZGx0IiwKICAgICAgICAic2NoZW1hXyI6ICJzYW5kYm94IiwKICAgICAgICAidHlwZSI6ICJEQVRBQlJJQ0tTX0RMVCIKICAgIH0KfQ==",
        },
    }

    # Test resources
    resources = pl.core_resources
    assert len(resources) == 4

    dlt = resources[0]
    dltp = resources[1]

    assert isinstance(dlt, models.resources.databricks.DLTPipeline)
    assert isinstance(dltp, models.resources.databricks.Permissions)

    assert dlt.resource_name == "dlt-pipeline-pl-dlt"
    assert dltp.resource_name == "permissions-dlt-pipeline-pl-dlt"

    assert dlt.options.provider == "${resources.databricks2}"
    assert dltp.options.provider == "${resources.databricks2}"

    assert dlt.options.depends_on == []
    assert dltp.options.depends_on == ["${resources.dlt-pipeline-pl-dlt}"]
