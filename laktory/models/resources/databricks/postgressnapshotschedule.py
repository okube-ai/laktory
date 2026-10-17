from pydantic import Field

from laktory.models.resources.databricks._postgrespath import postgres_path_key
from laktory.models.resources.databricks.postgressnapshotschedule_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgressnapshotschedule_base import (
    PostgresSnapshotScheduleBase,
)


class PostgresSnapshotSchedule(PostgresSnapshotScheduleBase):
    """
    Lakebase Autoscaling (Postgres) snapshot schedule

    Daily, weekly and/or monthly snapshots of a branch, each with its own
    retention. Can also be declared as the `snapshot_schedule` of a
    `PostgresBranch`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    schedule_yaml = '''
    parent: projects/app/branches/production
    schedule:
    - daily_schedule:
        hour: 3
      retention: 168h0m0s
    '''
    schedule = models.resources.databricks.PostgresSnapshotSchedule.model_validate_yaml(
        io.StringIO(schedule_yaml)
    )
    ```

    References
    ----------

    * [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/)
    * [Terraform databricks_postgres_snapshot_schedule](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_snapshot_schedule)
    """

    # Relax field the base marks required but Laktory fills from the parent branch
    parent: str = Field(
        None,
        description="Branch of the snapshots (`projects/{project_id}/branches/{branch_id}`). Set automatically when declared as the snapshot_schedule of a branch.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return postgres_path_key(self.parent)
