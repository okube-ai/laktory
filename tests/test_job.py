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
        ],
    )


if __name__ == "__main__":
    test_job()
