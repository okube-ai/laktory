"""
Tests for LakeflowJobOrchestrator.

Covers:
  - One task per execution_task_name (tasks group nodes sharing the same key)
  - Task dependency DAG matches pipeline node DAG
  - Library list in each task matches pipeline dependencies
  - to_dab_resource() returns a DABs Job
"""

from laktory import models


def _get_pl():
    return models.Pipeline(
        name="pl-job",
        databricks_quality_monitor_enabled=True,
        nodes=[
            models.PipelineNode(
                name="brz",
                source={"format": "JSON", "path": "/brz_source/"},
                sinks=[{"format": "PARQUET", "mode": "APPEND", "path": "/brz_sink/"}],
            ),
            models.PipelineNode(
                name="slv",
                source={"node_name": "brz"},
                sinks=[{"format": "DELTA", "mode": "APPEND", "path": "/slv_sink/"}],
            ),
            models.PipelineNode(
                name="gld",
                source={"node_name": "slv"},
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
