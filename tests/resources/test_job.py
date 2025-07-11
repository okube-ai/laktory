from laktory.models.resources.databricks import Job

job = Job(
    name="job-stock-prices",
    name_prefix="osoucy]",
    job_clusters=[
        {
            "job_cluster_key": "main",
            "new_cluster": {
                "spark_version": "16.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
            },
        },
    ],
    tasks=[
        {
            "depends_ons": [{"task_key": "ingestion"}],
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
        "on_duration_warning_threshold_exceededs": ["info@okube.ai"],
        "on_failures": ["info@okube.ai"],
        "on_starts": ["info@okube.ai"],
        "on_successes": ["info@okube.ai"],
    },
)

job_for_each = Job(
    name="job-hello",
    tasks=[
        {
            "task_key": "hello-loop",
            "for_each_task": {
                "inputs": [{"id": 1, "name": "olivier"}, {"id": 2, "name": "kubic"}],
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
    data = job.model_dump()
    print(data)
    assert data == {
        "access_controls": [],
        "continuous": None,
        "control_run_state": None,
        "description": None,
        "email_notifications": {
            "no_alert_for_skipped_runs": None,
            "on_duration_warning_threshold_exceededs": ["info@okube.ai"],
            "on_failures": ["info@okube.ai"],
            "on_starts": ["info@okube.ai"],
            "on_successes": ["info@okube.ai"],
        },
        "environments": None,
        "format": None,
        "git_source": None,
        "health": None,
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
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": None,
                    "policy_id": None,
                    "runtime_engine": None,
                    "single_user_name": None,
                    "spark_conf": {},
                    "spark_env_vars": {},
                    "spark_version": "16.3.x-scala2.12",
                    "ssh_public_keys": [],
                },
            }
        ],
        "max_concurrent_runs": None,
        "max_retries": None,
        "min_retry_interval_millis": None,
        "name": "osoucy]job-stock-prices",
        "name_prefix": "",
        "name_suffix": None,
        "notification_settings": None,
        "parameters": [],
        "queue": None,
        "retry_on_timeout": None,
        "run_as": None,
        "schedule": None,
        "tags": {},
        "tasks": [
            {
                "dbt_task": None,
                "condition_task": None,
                "depends_ons": None,
                "description": None,
                "email_notifications": None,
                "environment_key": None,
                "existing_cluster_id": None,
                "health": None,
                "job_cluster_key": "main",
                "libraries": None,
                "max_retries": None,
                "min_retry_interval_millis": None,
                "notebook_task": {
                    "notebook_path": "job/ingest_stock_prices",
                    "base_parameters": None,
                    "warehouse_id": None,
                    "source": None,
                },
                "notification_settings": None,
                "pipeline_task": None,
                "python_wheel_task": None,
                "retry_on_timeout": None,
                "run_if": None,
                "run_job_task": None,
                "sql_task": None,
                "task_key": "ingestion",
                "timeout_seconds": None,
                "for_each_task": None,
            },
            {
                "dbt_task": None,
                "condition_task": None,
                "depends_ons": [{"task_key": "ingestion", "outcome": None}],
                "description": None,
                "email_notifications": None,
                "environment_key": None,
                "existing_cluster_id": None,
                "health": None,
                "job_cluster_key": None,
                "libraries": None,
                "max_retries": None,
                "min_retry_interval_millis": None,
                "notebook_task": None,
                "notification_settings": None,
                "pipeline_task": {
                    "pipeline_id": "${resources.dlt-pipeline-pl-dlt.id}",
                    "full_refresh": None,
                },
                "python_wheel_task": None,
                "retry_on_timeout": None,
                "run_if": None,
                "run_job_task": None,
                "sql_task": None,
                "task_key": "pipeline",
                "timeout_seconds": None,
                "for_each_task": None,
            },
            {
                "dbt_task": None,
                "condition_task": None,
                "depends_ons": None,
                "description": None,
                "email_notifications": None,
                "environment_key": None,
                "existing_cluster_id": None,
                "health": None,
                "job_cluster_key": None,
                "libraries": None,
                "max_retries": None,
                "min_retry_interval_millis": None,
                "notebook_task": None,
                "notification_settings": None,
                "pipeline_task": None,
                "python_wheel_task": None,
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
                "for_each_task": None,
            },
        ],
        "timeout_seconds": None,
        "trigger": None,
        "webhook_notifications": None,
    }


def test_job_for_each_task():
    data = job_for_each.model_dump(exclude_unset=False)

    print(data)
    assert data == {
        "access_controls": [],
        "continuous": None,
        "control_run_state": None,
        "description": None,
        "email_notifications": None,
        "environments": None,
        "format": None,
        "git_source": None,
        "health": None,
        "job_clusters": [],
        "max_concurrent_runs": None,
        "max_retries": None,
        "min_retry_interval_millis": None,
        "name": "job-hello",
        "name_prefix": None,
        "name_suffix": None,
        "notification_settings": None,
        "parameters": [],
        "queue": None,
        "retry_on_timeout": None,
        "run_as": None,
        "schedule": None,
        "tags": {},
        "tasks": [
            {
                "dbt_task": None,
                "condition_task": None,
                "depends_ons": None,
                "description": None,
                "email_notifications": None,
                "environment_key": None,
                "existing_cluster_id": None,
                "health": None,
                "job_cluster_key": None,
                "libraries": None,
                "max_retries": None,
                "min_retry_interval_millis": None,
                "notebook_task": None,
                "notification_settings": None,
                "pipeline_task": None,
                "python_wheel_task": None,
                "retry_on_timeout": None,
                "run_if": None,
                "run_job_task": None,
                "sql_task": None,
                "task_key": "hello-loop",
                "timeout_seconds": None,
                "for_each_task": {
                    "inputs": '[{"id": 1, "name": "olivier"}, {"id": 2, "name": "kubic"}]',
                    "task": {
                        "dbt_task": None,
                        "condition_task": None,
                        "depends_ons": None,
                        "description": None,
                        "email_notifications": None,
                        "environment_key": None,
                        "existing_cluster_id": None,
                        "health": None,
                        "job_cluster_key": None,
                        "libraries": None,
                        "max_retries": None,
                        "min_retry_interval_millis": None,
                        "notebook_task": {
                            "notebook_path": "Workspace/Users/olivier.soucy@okube.ai/hello-world",
                            "base_parameters": {"input": "{{input}}"},
                            "warehouse_id": None,
                            "source": None,
                        },
                        "notification_settings": None,
                        "pipeline_task": None,
                        "python_wheel_task": None,
                        "retry_on_timeout": None,
                        "run_if": None,
                        "run_job_task": None,
                        "sql_task": None,
                        "task_key": "hello-task",
                        "timeout_seconds": None,
                    },
                    "concurrency": None,
                },
            }
        ],
        "timeout_seconds": None,
        "trigger": None,
        "webhook_notifications": None,
    }


def test_job_pulumi():
    assert job.resource_name == "osoucy-job-stock-prices"
    assert job.options.model_dump(exclude_none=True) == {
        "depends_on": [],
        "delete_before_replace": True,
        "is_enabled": True,
    }
    data = job.pulumi_properties
    print(data)
    assert data == {
        "email_notifications": {
            "on_duration_warning_threshold_exceededs": ["info@okube.ai"],
            "on_failures": ["info@okube.ai"],
            "on_starts": ["info@okube.ai"],
            "on_successes": ["info@okube.ai"],
        },
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
                "pipeline_task": {"pipeline_id": "${resources.dlt-pipeline-pl-dlt.id}"},
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
                    "spark_version": "16.3.x-scala2.12",
                    "ssh_public_keys": [],
                },
            }
        ],
        "name": "osoucy]job-stock-prices",
    }


def test_job_task_dbt():
    job = Job(
        name="job-stock-prices",
        tasks=[
            {
                "dbt_task": {
                    "commands": ["dbt build", "dbt run"],
                    "schema_": "finance",
                },
            },
        ],
    )
    assert job.pulumi_properties == {
        "name": "job-stock-prices",
        "parameters": [],
        "tags": {},
        "tasks": [
            {"dbt_task": {"commands": ["dbt build", "dbt run"], "schema": "finance"}}
        ],
        "job_clusters": [],
    }


if __name__ == "__main__":
    test_job_model()
    test_job_for_each_task()
    test_job_pulumi()
    test_job_task_dbt()
