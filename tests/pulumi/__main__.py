import pulumi
import pulumi_databricks as databricks
from laktory import models


# --------------------------------------------------------------------------- #
# Service                                                                     #
# --------------------------------------------------------------------------- #


class Service:
    def __init__(self):
        self.org = "o3"
        self.service = "laktory"
        self.pulumi_config = pulumi.Config()
        self.env = pulumi.get_stack()

        # Providers
        self.workspace_provider = databricks.Provider(
            "provider-workspace-neptune",
            host="adb-2211091707396001.1.azuredatabricks.net",
        )

        self.pipelines = []
        self.jobs = []
        self.stack = None

    def run(self):
        self.set_pipelines()
        self.set_jobs()
        self.set_stack()

    # ----------------------------------------------------------------------- #
    # Pipelines                                                               #
    # ----------------------------------------------------------------------- #

    def set_pipelines(self):
        self.pipelines = [
            models.Pipeline(
                resource_name="pl-custom-name",
                name="pl-stock-prices",
                libraries=[
                    {"notebook": {"path": "/pipelines/dlt_brz_template.py"}},
                ],
                permissions=[
                    {"group_name": "account users", "permission_level": "CAN_VIEW"},
                    {"group_name": "role-engineers", "permission_level": "CAN_RUN"},
                ],
            )
        ]
        # self.pipelines[-1].deploy_with_pulumi(
        #     opts=pulumi.ResourceOptions(provider=self.workspace_provider)
        # )

    # ----------------------------------------------------------------------- #
    # Jobs                                                                    #
    # ----------------------------------------------------------------------- #

    def set_jobs(self):
        self.jobs = [
            models.Job(
                name="job-stock-prices",
                clusters=[
                    {
                        "name": "main",
                        "spark_version": "14.0.x-scala2.12",
                        "node_type_id": "Standard_DS3_v2",
                    }
                ],
                tasks=[
                    {
                        "task_key": "ingest-metadata",
                        "job_cluster_key": "main",
                        "notebook_task": {
                            "notebook_path": "/jobs/ingest_stock_metadata.py",
                        },
                        "libraries": [
                            {"pypi": {"package": "laktory==0.0.27"}},
                            {"pypi": {"package": "yfinance"}},
                        ],
                    },
                    {
                        "task_key": "run-pipeline",
                        "pipeline_task": {
                            "pipeline_id": "${pipelines.pl-custom-name.id}",
                        },
                        "libraries": [
                            {"pypi": {"package": "laktory==0.0.27"}},
                            {"pypi": {"package": "yfinance"}},
                        ],
                    },
                ],
            )
        ]
        # self.jobs[-1].deploy_with_pulumi(opts=pulumi.ResourceOptions(provider=self.workspace_provider))

    def set_stack(self):
        self.stack = models.Stack(
            name="unit-testing",
            config={
                "databricks:host": "adb-2211091707396001.1.azuredatabricks.net",
            },
            resources={
                "jobs": self.jobs,
                "pipelines": self.pipelines,
            },
        )
        self.stack.deploy_with_pulumi()


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    service = Service()
    service.run()
