import os

from laktory._testing.stackvalidator import StackValidator
from laktory.models import Job
from laktory import pulumi_outputs


root_dir = os.path.dirname(__file__)

job = Job(
    name="job-stock-prices",
    clusters=[
        {
            "name": "main",
            "spark_version": "14.0.x-scala2.12",
            "node_type_id": "Standard_DS3_v2",
        },
    ],
    tasks=[
        {
            "job_cluster_key": "main",
            "notebook_task": {
                "notebook_path": "job/ingest_stock_prices",
            },
            "task_key": "ingestion",
        },
        {
            "depends_ons": [{"task_key": "ingestion"}],
            "pipeline_task": {"pipeline_id": "${resources.pl-stock-prices.id}"},
            "task_key": "pipeline",
        },
        {
            "task_key": "view",
            "sql_task": {
                "query": {"query_id": "456"},
                "warehouse_id": "123",
            },
        },
    ],
)


def test_job_model():
    data = job.model_dump()
    print(data)
    assert data == {
        "access_controls": [],
        "clusters": [
            {
                "apply_policy_default_values": None,
                "autoscale": None,
                "autotermination_minutes": None,
                "cluster_id": None,
                "custom_tags": None,
                "data_security_mode": "USER_ISOLATION",
                "driver_instance_pool_id": None,
                "driver_node_type_id": None,
                "enable_elastic_disk": None,
                "enable_local_disk_encryption": None,
                "idempotency_token": None,
                "init_scripts": [],
                "instance_pool_id": None,
                "name": "main",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": None,
                "policy_id": None,
                "runtime_engine": None,
                "single_user_name": None,
                "spark_conf": {},
                "spark_env_vars": {},
                "spark_version": "14.0.x-scala2.12",
                "ssh_public_keys": [],
            }
        ],
        "continuous": None,
        "control_run_state": None,
        "email_notifications": None,
        "format": None,
        "health": None,
        "max_concurrent_runs": None,
        "max_retries": None,
        "min_retry_interval_millis": None,
        "name": "job-stock-prices",
        "notification_settings": None,
        "parameters": [],
        "retry_on_timeout": None,
        "run_as": None,
        "schedule": None,
        "tags": {},
        "tasks": [
            {
                "condition_task": None,
                "depends_ons": None,
                "description": None,
                "email_notifications": None,
                "existing_cluster_id": None,
                "health": None,
                "job_cluster_key": "main",
                "libraries": None,
                "max_retries": None,
                "min_retry_interval_millis": None,
                "notebook_task": {
                    "notebook_path": "job/ingest_stock_prices",
                    "base_parameters": None,
                    "source": None,
                },
                "notification_settings": None,
                "pipeline_task": None,
                "retry_on_timeout": None,
                "run_if": None,
                "run_job_task": None,
                "sql_task": None,
                "task_key": "ingestion",
                "timeout_seconds": None,
            },
            {
                "condition_task": None,
                "depends_ons": [{"task_key": "ingestion", "outcome": None}],
                "description": None,
                "email_notifications": None,
                "existing_cluster_id": None,
                "health": None,
                "job_cluster_key": None,
                "libraries": None,
                "max_retries": None,
                "min_retry_interval_millis": None,
                "notebook_task": None,
                "notification_settings": None,
                "pipeline_task": {
                    "pipeline_id": "${resources.pl-stock-prices.id}",
                    "full_refresh": None,
                },
                "retry_on_timeout": None,
                "run_if": None,
                "run_job_task": None,
                "sql_task": None,
                "task_key": "pipeline",
                "timeout_seconds": None,
            },
            {
                "condition_task": None,
                "depends_ons": None,
                "description": None,
                "email_notifications": None,
                "existing_cluster_id": None,
                "health": None,
                "job_cluster_key": None,
                "libraries": None,
                "max_retries": None,
                "min_retry_interval_millis": None,
                "notebook_task": None,
                "notification_settings": None,
                "pipeline_task": None,
                "retry_on_timeout": None,
                "run_if": None,
                "run_job_task": None,
                "sql_task": {
                    "alert": None,
                    "dashboard": None,
                    "file": None,
                    "parameters": None,
                    "query": {"query_id": "456"},
                    "warehouse_id": "123",
                },
                "task_key": "view",
                "timeout_seconds": None,
            },
        ],
        "timeout_seconds": None,
        "trigger": None,
        "webhook_notifications": None,
    }


def test_job_pulumi():
    pulumi_outputs["pl-stock-prices.id"] = "12345"
    assert job.resource_name == "job-stock-prices"
    assert job.options.model_dump(exclude_none=True) == {
        "depends_on": [],
        "delete_before_replace": True,
    }
    data = job.pulumi_properties
    print(data)
    assert data == {
        "name": "job-stock-prices",
        "parameters": [],
        "tags": {},
        "tasks": [
            {
                "job_cluster_key": "main",
                "notebook_task": {"notebook_path": "job/ingest_stock_prices"},
                "task_key": "ingestion",
            },
            {
                "depends_ons": [{"task_key": "ingestion"}],
                "pipeline_task": {"pipeline_id": "12345"},
                "task_key": "pipeline",
            },
            {
                "sql_task": {"query": {"query_id": "456"}, "warehouse_id": "123"},
                "task_key": "view",
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "main",
                "new_cluster": {
                    "data_security_mode": "USER_ISOLATION",
                    "init_scripts": [],
                    "node_type_id": "Standard_DS3_v2",
                    "spark_conf": {},
                    "spark_env_vars": {},
                    "spark_version": "14.0.x-scala2.12",
                    "ssh_public_keys": [],
                },
            }
        ],
    }


def test_deploy():
    validator = StackValidator({"jobs": [job]})
    validator.validate()


if __name__ == "__main__":
    test_job_model()
    test_job_pulumi()
    test_deploy()
