import pulumi
import pulumi_databricks as databricks
from laktory._settings import settings
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.compute.job import Job
from laktory.models.compute.pipeline import Pipeline

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiJob(PulumiResourcesEngine):
    @property
    def provider(self):
        return "databricks"

    def __init__(
        self,
        name=None,
        job: Job = None,
        opts=None,
    ):
        if name is None:
            name = f"job-{job.name}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        # ------------------------------------------------------------------- #
        # Job                                                                 #
        # ------------------------------------------------------------------- #

        self.job = databricks.Job(
            f"job-{job.name}",
            opts=opts,
            **job.model_pulumi_dump(),
        )

        access_controls = []
        for permission in job.permissions:
            access_controls += [
                databricks.PermissionsAccessControlArgs(
                    permission_level=permission.permission_level,
                    group_name=permission.group_name,
                    service_principal_name=permission.service_principal_name,
                    user_name=permission.user_name,
                )
            ]

        if access_controls:
            self.permissions = databricks.Permissions(
                f"permissions-job-{job.name}",
                access_controls=access_controls,
                job_id=self.job.id,
                opts=opts,
            )
