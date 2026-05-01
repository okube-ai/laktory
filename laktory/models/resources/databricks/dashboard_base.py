# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_dashboard
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class DashboardBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_dashboard`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    display_name: str = Field(..., description="The display name of the dashboard")
    warehouse_id: str = Field(
        ..., description="The warehouse ID used to run the dashboard"
    )
    create_time: str | None = Field(None)
    dashboard_change_detected: bool | None = Field(None)
    dashboard_id: str | None = Field(None)
    dataset_catalog: str | None = Field(
        None,
        description="Sets the default catalog for all datasets in this dashboard. Does not impact table references that use fully qualified catalog names (ex: samples.nyctaxi.trips)",
    )
    dataset_schema: str | None = Field(
        None,
        description="Sets the default schema for all datasets in this dashboard. Does not impact table references that use fully qualified catalog names (ex: samples.nyctaxi.trips)",
    )
    embed_credentials: bool | None = Field(
        None,
        description="Whether to embed credentials in the dashboard. Default is `true`",
    )
    etag: str | None = Field(None)
    file_path: str | None = Field(
        None,
        description="The path to the dashboard JSON file. Conflicts with `serialized_dashboard`",
    )
    lifecycle_state: str | None = Field(None)
    md5: str | None = Field(None)
    path: str | None = Field(None)
    serialized_dashboard: str | None = Field(
        None,
        description="The contents of the dashboard in serialized string form. Conflicts with `file_path`",
    )
    update_time: str | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_dashboard"


__all__ = ["DashboardBase"]
