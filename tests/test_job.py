import os
from laktory.models import Job
from laktory import pulumi_outputs

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
    pulumi_outputs["pl-stock-prices.id"] = "12345"
    data = job.pulumi_properties
    print(data)
    assert data == {
        "options": {"depends_on": [], "delete_before_replace": True},
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
                    "options": {"depends_on": [], "delete_before_replace": True},
                    "data_security_mode": "USER_ISOLATION",
                    "init_scripts": [],
                    "node_type_id": "Standard_DS3_v2",
                    "spark_conf": {},
                    "spark_env_vars": {},
                    "spark_version": "14.0.x-scala2.12",
                    "ssh_public_keys": [],
                    "resource_name": "job-cluster-main",
                },
            }
        ],
    }


if __name__ == "__main__":
    test_job()
