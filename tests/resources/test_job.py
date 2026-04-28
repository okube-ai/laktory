from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Job
from laktory.models.resources.databricks.permissions import Permissions

job = Job(
    name="job-stock-prices",
    name_prefix="osoucy]",
    job_clusters=[
        {
            "job_cluster_key": "main",
            "new_cluster": {
                "spark_version": "16.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "init_scripts": [{"volumes": {"destination": "Volumes/some/path"}}],
            },
        },
    ],
    tasks=[
        {
            "depends_on": [{"task_key": "ingestion"}],
            "pipeline_task": {"pipeline_id": "${resources.dlt-pipeline-pl-dlt.id}"},
            "task_key": "pipeline",
        },
        {
            "task_key": "view",
            "sql_task": {
                "query": {"query_id": "456"},
                "warehouse_id": "123",
            },
        },
        {
            "job_cluster_key": "main",
            "notebook_task": {
                "notebook_path": "job/ingest_stock_prices",
            },
            "task_key": "ingestion",
        },
    ],
    email_notifications={
        "on_duration_warning_threshold_exceeded": ["info@okube.ai"],
        "on_failure": ["info@okube.ai"],
        "on_start": ["info@okube.ai"],
        "on_success": ["info@okube.ai"],
    },
)

job_for_each = Job(
    name="job-hello",
    tasks=[
        {
            "task_key": "hello-loop",
            "for_each_task": {
                "inputs": '[{"id": 1, "name": "olivier"}, {"id": 2, "name": "kubic"}]',
                "task": {
                    "task_key": "hello-task",
                    "notebook_task": {
                        "notebook_path": "Workspace/Users/olivier.soucy@okube.ai/hello-world",
                        "base_parameters": {"input": "{{input}}"},
                    },
                },
            },
        }
    ],
)


def test_job_model():
    assert job.name == "osoucy]job-stock-prices"
    assert job.job_cluster[0].job_cluster_key == "main"
    assert job.email_notifications.on_failure == ["info@okube.ai"]


def test_job_for_each_task():
    assert job_for_each.name == "job-hello"
    t = job_for_each.task[0]
    assert t.task_key == "hello-loop"
    assert t.for_each_task is not None
    assert t.for_each_task.task.task_key == "hello-task"


def test_job_task_dbt():
    j = Job(
        name="job-stock-prices",
        tasks=[
            {
                "task_key": "dbt-task",
                "dbt_task": {
                    "commands": ["dbt build", "dbt run"],
                    "schema_": "finance",
                },
            },
        ],
    )
    assert j.terraform_properties == {
        "name": "job-stock-prices",
        "task": [
            {
                "task_key": "dbt-task",
                "dbt_task": {"commands": ["dbt build", "dbt run"], "schema": "finance"},
            }
        ],
    }


def test_job_additional_resources():
    j = Job(
        name="job-test",
        tasks=[{"task_key": "t1", "notebook_task": {"notebook_path": "/nb"}}],
        access_controls=[{"group_name": "users", "permission_level": "CAN_VIEW"}],
    )
    assert len(j.additional_core_resources) == 1
    assert isinstance(j.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    job_simple = Job(
        name="job-simple",
        tasks=[
            {"task_key": "t1", "notebook_task": {"notebook_path": "/notebooks/test"}}
        ],
    )
    plan_resource(job_simple)
