import pulumi
import pulumi_databricks as databricks
from laktory._settings import settings
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.compute.job import Job

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
            continuous=job.continuous,
            control_run_state=job.control_run_state,
            email_notifications=getattr(job.email_notifications, "pulumi_args", None),
            format=job.format,
            health=getattr(job.health, "pulumi_args", None),
            job_clusters=[v.pulumi_args for v in job.clusters],
            max_concurrent_runs=job.max_concurrent_runs,
            max_retries=job.max_retries,
            min_retry_interval_millis=job.min_retry_interval_millis,
            name=job.name,
            notification_settings=getattr(job.notification_settings, "pulumi_args", None),
            parameters=[v.pulumi_args for v in job.parameters],
            # queue=job.queue,
            retry_on_timeout=job.retry_on_timeout,
            run_as=getattr(job.run_as, "pulumi_args", None),
            schedule=getattr(job.schedule, "pulumi_args", None),
            tags=job.tags,
            tasks=[v.pulumi_args for v in job.tasks],
            timeout_seconds=job.timeout_seconds,
            trigger=getattr(job.trigger, "pulumi_args", None),
            webhook_notifications=getattr(job.webhook_notifications, "pulumi_args", None),
            opts=opts,
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
