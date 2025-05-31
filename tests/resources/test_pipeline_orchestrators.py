from __future__ import annotations

import io
import json
from pathlib import Path

from laktory import __version__
from laktory import models
from laktory.enums import DataFrameBackends

data_dirpath = Path(__file__).parent.parent / "data"
testdir_path = Path(__file__).parent


def get_pl(tmp_path=""):
    with open(data_dirpath / "pl.yaml") as fp:
        data = fp.read()
        data = data.replace("{tmp_path}", str(tmp_path))
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))

        # Delete Views (not supported by DLT)
        del pl.nodes[-1]
        del pl.nodes[-1]
        del pl.nodes[0]

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
                "spark_version": "14.0.x-scala2.12",
            }
        ],
        "name": "pl-job",
        "type": "DATABRICKS_JOB",
    }

    return pl


# DLT
def get_pl_dlt():
    pl = get_pl()
    pl.name = "pl-dlt"
    o = models.DatabricksDLTOrchestrator(
        name="pl-dlt",
        catalog="dev",
        target="sandbox",
        access_controls=[
            {"group_name": "account users", "permission_level": "CAN_VIEW"}
        ],
        options={"provider": "${resources.databricks2}"},
    )
    pl.orchestrator = o
    pl.options = {"provider": "${resources.databricks1}"}

    return pl


def test_pipeline_job():
    # Test job
    job = get_pl_job().orchestrator
    data = job.model_dump(exclude_unset=True)
    data = json.loads(
        json.dumps(data).replace(f"laktory=={__version__}", "laktory==__version__")
    )
    assert data == {
        "clusters": [
            {
                "name": "node-cluster",
                "node_type_id": "Standard_DS3_v2",
                "spark_version": "14.0.x-scala2.12",
            }
        ],
        "environments": [
            {
                "environment_key": "laktory",
                "spec": {
                    "client": "2",
                    "dependencies": ["yfinance", "laktory==__version__"],
                },
            }
        ],
        "name": "pl-job",
        "parameters": [
            {"default": "false", "name": "full_refresh"},
            {"default": "pl-job", "name": "pipeline_name"},
            {"default": "false", "name": "install_dependencies"},
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
                    "base_parameters": {"node_name": "brz"},
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
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
                    "base_parameters": {"node_name": "gld"},
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                },
                "task_key": "node-gld",
            },
            {
                "depends_ons": [{"task_key": "node-brz"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "yfinance"}},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "notebook_task": {
                    "base_parameters": {"node_name": "slv"},
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                },
                "task_key": "node-slv",
            },
        ],
        "type": "DATABRICKS_JOB",
    }

    # Test resources
    resources = job.core_resources
    assert len(resources) == 5


def test_pipeline_dlt(tmp_path):
    pl_dlt = get_pl_dlt()

    # Test Sink as Source
    node_slv = pl_dlt.nodes_dict["slv"]
    sink_source = node_slv.source.node.primary_sink.as_source(
        as_stream=node_slv.source.as_stream
    )
    data = sink_source.model_dump()
    assert data.pop("dataframe_backend") == DataFrameBackends.PYSPARK
    assert data == {
        "dataframe_api": None,
        "as_stream": False,
        "drop_duplicates": None,
        "drops": None,
        "filter": None,
        "renames": None,
        "selects": None,
        "type": "FILE",
        "format": "PARQUET",
        "has_header": True,
        "infer_schema": False,
        "path": "/brz_sink/",
        "reader_kwargs": {},
        "schema_definition": None,
        "schema_location": None,
        "reader_methods": [],
    }

    data = pl_dlt.orchestrator.model_dump()
    assert data == {
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
        "config_file": {
            "access_controls": [
                {
                    "group_name": "users",
                    "permission_level": "CAN_READ",
                    "service_principal_name": None,
                    "user_name": None,
                }
            ],
            "dataframe_api": None,
            "dataframe_backend": None,
            "dirpath": "",
            "path": "/.laktory/pipelines/pl-dlt/config.json",
            "rootpath": "/.laktory/",
            "source": "./tmp-pl-dlt-config.json",
        },
        "configuration": {
            "pipeline_name": "pl-dlt",
            "workspace_laktory_root": "/.laktory/",
        },
        "continuous": None,
        "dataframe_api": None,
        "dataframe_backend": None,
        "development": None,
        "edition": None,
        "libraries": None,
        "name": "pl-dlt",
        "name_prefix": None,
        "name_suffix": None,
        "notifications": [],
        "photon": None,
        "requirements_file": {
            "access_controls": [
                {
                    "group_name": "users",
                    "permission_level": "CAN_READ",
                    "service_principal_name": None,
                    "user_name": None,
                }
            ],
            "dataframe_api": None,
            "dataframe_backend": None,
            "dirpath": "",
            "path": "/.laktory/pipelines/pl-dlt/requirements.txt",
            "rootpath": "/.laktory/",
            "source": "./tmp-pl-dlt-requirements.txt",
        },
        "serverless": None,
        "storage": None,
        "target": "sandbox",
        "type": "DATABRICKS_DLT",
    }

    # Test resources
    resources = pl_dlt.core_resources
    assert len(resources) == 6

    dlt = resources[0]
    dltp = resources[1]
    wsf = resources[2]
    wsfp = resources[3]

    assert isinstance(wsf, models.resources.databricks.WorkspaceFile)
    assert isinstance(wsfp, models.resources.databricks.Permissions)
    assert isinstance(dlt, models.resources.databricks.DLTPipeline)
    assert isinstance(dltp, models.resources.databricks.Permissions)

    assert dlt.resource_name == "dlt-pipeline-pl-dlt"
    assert dltp.resource_name == "permissions-dlt-pipeline-pl-dlt"
    assert wsf.resource_name == "workspace-file-laktory-pipelines-pl-dlt-config-json"
    assert (
        wsfp.resource_name
        == "permissions-workspace-file-laktory-pipelines-pl-dlt-config-json"
    )

    assert dlt.options.provider == "${resources.databricks2}"
    assert dltp.options.provider == "${resources.databricks2}"
    assert wsf.options.provider == "${resources.databricks2}"
    assert wsfp.options.provider == "${resources.databricks2}"

    assert dlt.options.depends_on == []
    assert dltp.options.depends_on == ["${resources.dlt-pipeline-pl-dlt}"]
    assert wsf.options.depends_on == ["${resources.dlt-pipeline-pl-dlt}"]
    assert wsfp.options.depends_on == [
        "${resources.workspace-file-laktory-pipelines-pl-dlt-config-json}"
    ]
