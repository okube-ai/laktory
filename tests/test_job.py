import os
from datetime import datetime

from laktory.models import Catalog
from laktory.models import Table
from laktory.models import Job
from laktory.models import Pipeline
from laktory._testing import StockPricesPipeline

root_dir = os.path.dirname(__file__)


def test_job():
    job = Job(
        name="job-stock-prices",
        tasks=[
            {
                "compute_key": "main",
                "notebook_task": {
                    "notebook_path": "job/ingest_stock_prices",
                },
                "task_key": "ingestion",
            },
            {
                "depends_ons": [{"task_key": "ingestion"}],
                "pipeline_task": {
                    "pipeline_id": "TBD"
                },
                "task_key": "pipeline",
            }
        ]
    )


if __name__ == "__main__":
    test_job()
