import os
import io
from pathlib import Path
from laktory import models
from laktory._testing import Paths

paths = Paths(__file__)
testdir_path = Path(__file__).parent


def get_pl(extra=None):
    with open(os.path.join(paths.data, "pl-spark-local.yaml"), "r") as fp:
        data = fp.read()
        data = data.replace("{data_dir}", "data")
        data = data.replace("{pl_dir}", "")
        if extra is not None:
            data += extra
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))
    return pl


# Job
pl_job = get_pl(
    """
name: pl-spark-job
orchestrator: DATABRICKS_JOB
databricks_job:
  name: job-pl-stock-prices
  laktory_version: 0.3.0
  clusters:
    - name: node-cluster
      spark_version: 14.0.x-scala2.12
      node_type_id: Standard_DS3_v2
"""
)

# DLT
pl_dlt = get_pl(
    """
name: pl-spark-dlt
orchestrator: DLT
dlt:
  catalog: dev
  target: sandbox
  access_controls:
  - group_name: account users
    permission_level: CAN_VIEW
  options:
    provider: ${resources.databricks2}
options:
  provider: ${resources.databricks1}
"""
)


def test_pipeline_job():

    # Test job
    job = pl_job.databricks_job
    data = job.model_dump(exclude_unset=True)
    print(data)
    assert data == {
        "clusters": [
            {
                "name": "node-cluster",
                "node_type_id": "Standard_DS3_v2",
                "spark_version": "14.0.x-scala2.12",
            }
        ],
        "name": "job-pl-stock-prices",
        "parameters": [
            {"default": "pl-spark-job", "name": "pipeline_name"},
            {"default": "false", "name": "full_refresh"},
        ],
        "tasks": [
            {
                "depends_ons": [],
                "job_cluster_key": "node-cluster",
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "brz_stock_meta"},
                },
                "task_key": "node-brz_stock_meta",
            },
            {
                "depends_ons": [],
                "job_cluster_key": "node-cluster",
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "brz_stock_prices"},
                },
                "task_key": "node-brz_stock_prices",
            },
            {
                "depends_ons": [{"task_key": "node-slv_stock_prices"}],
                "job_cluster_key": "node-cluster",
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "gld_stock_prices"},
                },
                "task_key": "node-gld_stock_prices",
            },
            {
                "depends_ons": [{"task_key": "node-brz_stock_meta"}],
                "job_cluster_key": "node-cluster",
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "slv_stock_meta"},
                },
                "task_key": "node-slv_stock_meta",
            },
            {
                "depends_ons": [
                    {"task_key": "node-brz_stock_prices"},
                    {"task_key": "node-slv_stock_meta"},
                ],
                "job_cluster_key": "node-cluster",
                "notebook_task": {
                    "notebook_path": "/.laktory/jobs/job_laktory_pl.py",
                    "base_parameters": {"node_name": "slv_stock_prices"},
                },
                "task_key": "node-slv_stock_prices",
            },
        ],
        "laktory_version": "0.3.0",
    }

    # Test resources
    resources = pl_job.core_resources
    assert len(resources) == 3


def test_pipeline_dlt():

    # Test Sink as Source
    sink_source = pl_dlt.nodes[1].source.node.primary_sink.as_source(
        as_stream=pl_dlt.nodes[1].source.as_stream
    )
    data = sink_source.model_dump()
    print(data)
    assert data == {
        "as_stream": True,
        "broadcast": False,
        "cdc": None,
        "dataframe_type": "SPARK",
        "drops": None,
        "filter": None,
        "limit": None,
        "renames": None,
        "sample": None,
        "selects": None,
        "watermark": None,
        "format": "DELTA",
        "header": True,
        "multiline": False,
        "path": "/brz_stock_prices",
        "read_options": {},
        "schema_location": None,
    }

    data = pl_dlt.dlt.model_dump()
    print(data)
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
        "configuration": {},
        "continuous": None,
        "development": None,
        "edition": None,
        "libraries": None,
        "name": "pl-spark-dlt",
        "notifications": [],
        "photon": None,
        "serverless": None,
        "storage": None,
        "target": "sandbox",
    }

    # Test resources
    resources = pl_dlt.core_resources
    assert len(resources) == 4

    wsf = resources[0]
    wsfp = resources[1]
    dlt = resources[2]
    dltp = resources[3]

    assert isinstance(wsf, models.resources.databricks.WorkspaceFile)
    assert isinstance(wsfp, models.resources.databricks.Permissions)
    assert isinstance(dlt, models.resources.databricks.DLTPipeline)
    assert isinstance(dltp, models.resources.databricks.Permissions)

    assert dlt.resource_name == "pl-spark-dlt"
    assert dltp.resource_name == "permissions-pl-spark-dlt"
    assert wsf.resource_name == "workspace-file-laktory-pipelines-pl-spark-dlt-json"
    assert (
        wsfp.resource_name
        == "permissions-workspace-file-laktory-pipelines-pl-spark-dlt-json"
    )

    assert dlt.options.provider == "${resources.databricks2}"
    assert dltp.options.provider == "${resources.databricks2}"
    assert wsf.options.provider == "${resources.databricks1}"
    assert wsfp.options.provider == "${resources.databricks1}"

    assert dlt.options.depends_on == []
    assert dltp.options.depends_on == ["${resources.pl-spark-dlt}"]
    assert wsf.options.depends_on == []
    assert wsfp.options.depends_on == [
        "${resources.workspace-file-laktory-pipelines-pl-spark-dlt-json}"
    ]


if __name__ == "__main__":
    test_pipeline_job()
    test_pipeline_dlt()
