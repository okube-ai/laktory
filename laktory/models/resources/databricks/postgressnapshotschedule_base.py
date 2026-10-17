# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_postgres_snapshot_schedule
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class PostgresSnapshotScheduleProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PostgresSnapshotScheduleScheduleDailySchedule(BaseModel):
    hour: int | None = Field(
        None,
        description="The hour of the day, in UTC, at which to take the snapshot, in [0, 23]",
    )


class PostgresSnapshotScheduleScheduleMonthlySchedule(BaseModel):
    day: int = Field(
        ...,
        description="The day of the month on which to take the snapshot, in [1, 31]. In shorter months the snapshot is taken on the last day instead (day 31 runs on Feb 28 or 29, and on Apr 30), so every month gets exactly one snapshot",
    )
    hour: int | None = Field(
        None,
        description="The hour of the day, in UTC, at which to take the snapshot, in [0, 23]",
    )


class PostgresSnapshotScheduleScheduleWeeklySchedule(BaseModel):
    day_of_week: str = Field(
        ...,
        description="The day of the week on which to take the snapshot. Possible values are: `FRIDAY`, `MONDAY`, `SATURDAY`, `SUNDAY`, `THURSDAY`, `TUESDAY`, `WEDNESDAY`",
    )
    hour: int | None = Field(
        None,
        description="The hour of the day, in UTC, at which to take the snapshot, in [0, 23]",
    )


class PostgresSnapshotScheduleSchedule(BaseModel):
    daily_schedule: PostgresSnapshotScheduleScheduleDailySchedule | None = Field(
        None, description="Take a snapshot once per day"
    )
    monthly_schedule: PostgresSnapshotScheduleScheduleMonthlySchedule | None = Field(
        None, description="Take a snapshot once per month"
    )
    retention: str = Field(
        ...,
        description="How long snapshots from this cadence are kept before automatic deletion. Must be at least 1 hour. Applied when a snapshot is taken; not retroactive, so changing it affects only later snapshots",
    )
    weekly_schedule: PostgresSnapshotScheduleScheduleWeeklySchedule | None = Field(
        None, description="Take a snapshot once per week"
    )


class PostgresSnapshotScheduleBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_postgres_snapshot_schedule`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    parent: str = Field(..., description="The resource name of the parent")
    provider_config: PostgresSnapshotScheduleProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    schedule: list[PostgresSnapshotScheduleSchedule] | None = PluralField(
        None,
        plural="schedules",
        description="The cadences at which automatic snapshots are taken. Update replaces the whole set; an empty set disables automatic snapshots. Order is not significant. When several cadences fire together, one snapshot is taken, retained for the longest of their retentions",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_postgres_snapshot_schedule"


__all__ = [
    "PostgresSnapshotScheduleBase",
    "PostgresSnapshotScheduleProviderConfig",
    "PostgresSnapshotScheduleSchedule",
    "PostgresSnapshotScheduleScheduleDailySchedule",
    "PostgresSnapshotScheduleScheduleMonthlySchedule",
    "PostgresSnapshotScheduleScheduleWeeklySchedule",
]
