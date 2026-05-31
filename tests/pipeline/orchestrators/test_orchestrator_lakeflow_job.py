"""
Tests for LakeflowJobOrchestrator.

Covers:
  - One task per execution_task_name (tasks group nodes sharing the same key)
  - Task dependency DAG matches pipeline node DAG
  - Library list in each task matches pipeline dependencies
  - to_dab_resource() returns a DABs Job
  - Serverless: environment populated, tasks use environment_key, missing version raises
  - Interactive cluster: tasks use job_cluster_key, .whl deps use whl library type,
    wrong cluster name raises
"""

import pytest
from pydantic import ValidationError

from laktory import models

_NODES = [
    {
        "name": "brz",
        "sources": {"df": {"format": "JSON", "path": "/brz_source/"}},
        "sinks": [{"format": "PARQUET", "mode": "APPEND", "path": "/brz_sink/"}],
    },
    {
        "name": "slv",
        "sources": {"df": {"node_name": "brz"}},
        "sinks": [{"format": "DELTA", "mode": "APPEND", "path": "/slv_sink/"}],
    },
]

_CLUSTER = {
    "job_cluster_key": "node-cluster",
    "new_cluster": {
        "node_type_id": "Standard_DS3_v2",
        "spark_version": "16.3.x-scala2.12",
    },
}


def _get_pl():
    return models.Pipeline(
        name="pl-job",
        databricks_quality_monitor_enabled=True,
        nodes=[
            models.PipelineNode(
                name="brz",
                sources={"df": {"format": "JSON", "path": "/brz_source/"}},
                sinks=[{"format": "PARQUET", "mode": "APPEND", "path": "/brz_sink/"}],
            ),
            models.PipelineNode(
                name="slv",
                sources={"df": {"node_name": "brz"}},
                sinks=[{"format": "DELTA", "mode": "APPEND", "path": "/slv_sink/"}],
            ),
            models.PipelineNode(
                name="gld",
                sources={"df": {"node_name": "slv"}},
                sinks=[
                    {"format": "PARQUET", "mode": "OVERWRITE", "path": "/gld_sink/"}
                ],
            ),
        ],
        orchestrator={
            "type": "LAKEFLOW_JOB",
            "name": "pl-job",
            "job_clusters": [
                {
                    "job_cluster_key": "node-cluster",
                    "new_cluster": {
                        "node_type_id": "Standard_DS3_v2",
                        "spark_version": "16.3.x-scala2.12",
                    },
                }
            ],
        },
    )


# --------------------------------------------------------------------------- #
# Task structure                                                              #
# --------------------------------------------------------------------------- #


def test_task_per_execution_task_name():
    pl = _get_pl()
    job = pl.orchestrator
    pl.get_execution_plan()

    task_keys = [t.task_key for t in job.task]
    # Each node (brz, slv, gld) gets its own task + post-execute task
    assert "node-brz" in task_keys
    assert "node-slv" in task_keys
    assert "node-gld" in task_keys
    assert "post-execute" in task_keys


def test_task_dependency_matches_dag():
    pl = _get_pl()
    job = pl.orchestrator

    tasks_dict = {t.task_key: t for t in job.task}

    # brz has no upstream deps
    brz_deps = [d.task_key for d in tasks_dict["node-brz"].depends_on]
    assert brz_deps == []

    # slv depends on brz
    slv_deps = [d.task_key for d in tasks_dict["node-slv"].depends_on]
    assert "node-brz" in slv_deps

    # gld depends on slv
    gld_deps = [d.task_key for d in tasks_dict["node-gld"].depends_on]
    assert "node-slv" in gld_deps


def test_library_list():
    pl = _get_pl()
    job = pl.orchestrator

    tasks_dict = {t.task_key: t for t in job.task}
    brz_task = tasks_dict["node-brz"]

    lib_packages = [
        lib.pypi.package for lib in brz_task.library if lib.pypi is not None
    ]
    assert any("laktory" in p for p in lib_packages)


def test_task_entry_point():
    pl = _get_pl()
    job = pl.orchestrator

    tasks_dict = {t.task_key: t for t in job.task}
    brz_task = tasks_dict["node-brz"]

    assert brz_task.python_wheel_task.package_name == "laktory"
    assert brz_task.python_wheel_task.entry_point == "models.pipeline._execute"


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# Deployed resources: config_file                                             #
# --------------------------------------------------------------------------- #


def test_config_file_in_additional_core_resources():
    """config_file is declared as an additional core resource of the orchestrator."""
    from laktory.models.pipeline.orchestrators.pipelineconfigworkspacefile import (
        PipelineConfigWorkspaceFile,
    )

    pl = _get_pl()
    additional = pl.orchestrator.additional_core_resources
    config_files = [r for r in additional if isinstance(r, PipelineConfigWorkspaceFile)]
    assert len(config_files) == 1


def test_config_file_path_contains_pipeline_name():
    """config_file workspace path encodes the pipeline name and ends with .json."""
    pl = _get_pl()
    cf = pl.orchestrator.config_file
    assert pl.name in cf.path
    assert cf.path.endswith(".json")


# --------------------------------------------------------------------------- #
# DABs resource                                                               #
# --------------------------------------------------------------------------- #


def test_to_dab_resource_returns_job_type(monkeypatch, tmp_path):
    from databricks.bundles.jobs import Job as DabsJob

    from laktory._settings import settings

    monkeypatch.setattr(settings, "build_root", str(tmp_path))
    pl = _get_pl()
    resource = pl.orchestrator.to_dab_resource()
    assert isinstance(resource, DabsJob)


def test_to_dab_resource_preserves_name(monkeypatch, tmp_path):
    from laktory._settings import settings

    monkeypatch.setattr(settings, "build_root", str(tmp_path))
    pl = _get_pl()
    resource = pl.orchestrator.to_dab_resource()
    assert resource.as_dict().get("name") == pl.orchestrator.name


# --------------------------------------------------------------------------- #
# Serverless: environment setup                                               #
# --------------------------------------------------------------------------- #


def test_serverless_missing_env_version_raises():
    """Serverless without serverless_environment_version must raise."""
    with pytest.raises(
        (ValueError, ValidationError), match="serverless_environment_version"
    ):
        models.Pipeline(
            name="pl-job-serverless",
            nodes=_NODES,
            orchestrator={
                "type": "LAKEFLOW_JOB",
                "name": "pl-job-serverless",
                # no job_clusters → serverless=True, no version → ValueError
            },
        )


def test_serverless_environment_populated():
    """Environment entry with key 'laktory' is added, dependencies include laktory, version matches."""
    pl = models.Pipeline(
        name="pl-job-serverless",
        nodes=_NODES,
        orchestrator={
            "type": "LAKEFLOW_JOB",
            "name": "pl-job-serverless",
            "serverless_environment_version": "5",
        },
    )
    job = pl.orchestrator
    env_keys = [e.environment_key for e in (job.environment or [])]
    assert "laktory" in env_keys

    laktory_env = next(e for e in job.environment if e.environment_key == "laktory")
    assert laktory_env.spec.environment_version == "5"
    assert any("laktory" in d for d in laktory_env.spec.dependencies)


def test_serverless_task_uses_environment_key():
    """In serverless mode each task gets environment_key='laktory', not job_cluster_key."""
    pl = models.Pipeline(
        name="pl-job-serverless",
        nodes=_NODES,
        orchestrator={
            "type": "LAKEFLOW_JOB",
            "name": "pl-job-serverless",
            "serverless_environment_version": "5",
        },
    )
    for task in pl.orchestrator.task:
        if task.task_key == "post-execute":
            continue
        assert task.environment_key == "laktory"
        assert task.job_cluster_key is None


# --------------------------------------------------------------------------- #
# Interactive cluster: compute and library setup                              #
# --------------------------------------------------------------------------- #


def test_cluster_task_uses_job_cluster_key():
    """In cluster mode each task gets job_cluster_key='node-cluster', not environment_key."""
    pl = _get_pl()
    for task in pl.orchestrator.task:
        if task.task_key == "post-execute":
            continue
        assert task.job_cluster_key == "node-cluster"
        assert task.environment_key is None


def test_cluster_whl_library_type():
    """.whl dependencies are added as whl libraries, not pypi."""
    pl = models.Pipeline(
        name="pl-job-cluster",
        nodes=_NODES,
        dependencies=["laktory", "/dbfs/my_lib-1.0.0-py3-none-any.whl"],
        orchestrator={
            "type": "LAKEFLOW_JOB",
            "name": "pl-job-cluster",
            "job_clusters": [_CLUSTER],
        },
    )
    tasks_dict = {t.task_key: t for t in pl.orchestrator.task}
    libs = tasks_dict["node-brz"].library

    whl_libs = [lib.whl for lib in libs if lib.whl is not None]
    pypi_packages = [lib.pypi.package for lib in libs if lib.pypi is not None]

    assert any("my_lib" in w for w in whl_libs)
    assert not any("my_lib" in p for p in pypi_packages)


def test_cluster_wrong_name_raises():
    """A job_cluster not named 'node-cluster' must raise."""
    with pytest.raises((ValueError, ValidationError), match="node-cluster"):
        models.Pipeline(
            name="pl-job-cluster",
            nodes=_NODES,
            orchestrator={
                "type": "LAKEFLOW_JOB",
                "name": "pl-job-cluster",
                "job_clusters": [
                    {
                        "job_cluster_key": "wrong-name",
                        "new_cluster": {
                            "node_type_id": "Standard_DS3_v2",
                            "spark_version": "16.3.x-scala2.12",
                        },
                    }
                ],
            },
        )
