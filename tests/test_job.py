import os
from laktory.models import Job

root_dir = os.path.dirname(__file__)


def test_job():
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
                "pipeline_task": {"pipeline_id": "TBD"},
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
    data = job.model_pulumi_dump()
    print(data)
    assert data == {
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
                "pipeline_task": {"pipeline_id": "TBD", "full_refresh": None},
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
        "job_clusters": [
            {
                "job_cluster_key": "main",
                "new_cluster": {
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
                    "is_pinned": None,
                    "libraries": None,
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": None,
                    "permissions": None,
                    "policy_id": None,
                    "runtime_engine": None,
                    "single_user_name": None,
                    "spark_conf": {},
                    "spark_env_vars": {},
                    "spark_version": "14.0.x-scala2.12",
                    "ssh_public_keys": [],
                },
            }
        ],
    }


if __name__ == "__main__":
    test_job()
