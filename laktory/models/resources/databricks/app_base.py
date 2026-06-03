# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_app
from __future__ import annotations

from typing import Any

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AppBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_app`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(..., description="The name of the app to grant permission on")
    budget_policy_id: str | None = Field(
        None, description="The Budget Policy ID set for this resource"
    )
    compute_max_instances: int | None = Field(None)
    compute_min_instances: int | None = Field(None)
    compute_size: str | None = Field(
        None,
        description="A string specifying compute size for the App. Possible values are `MEDIUM`, `LARGE`",
    )
    description: str | None = Field(None, description="The description of the resource")
    git_repository: Any | None = Field(
        None,
        description="Git repository configuration for app deployments (see [below](#git_repository-configuration-attribute)). When specified, deployments can reference code from this repository by providing only the git reference (branch, tag, or commit)",
    )
    no_compute: bool | None = Field(None)
    provider_config: Any | None = Field(None)
    resources: Any | None = Field(
        None, description="A list of resources that the app have access to"
    )
    space: str | None = Field(None)
    telemetry_export_destinations: Any | None = Field(
        None,
        description="A list of destinations to which the app's telemetry (logs, metrics, traces) is exported (see [below](#telemetry_export_destinations-configuration-attribute))",
    )
    usage_policy_id: str | None = Field(
        None, description="The Usage Policy ID set for this resource"
    )
    user_api_scopes: list[str] | None = Field(
        None,
        description="A list of api scopes granted to the user access token.  See [REST API docs](https://docs.databricks.com/api/workspace/api/scopes) for full list of supported scopes",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_app"


__all__ = ["AppBase"]
