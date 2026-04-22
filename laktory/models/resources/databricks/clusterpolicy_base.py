# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_cluster_policy
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class ClusterPolicyLibrariesCran(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class ClusterPolicyLibrariesMaven(BaseModel):
    coordinates: str = Field(...)
    exclusions: list[str] | None = Field(None)
    repo: str | None = Field(None)


class ClusterPolicyLibrariesPypi(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class ClusterPolicyLibraries(BaseModel):
    egg: str | None = Field(None)
    jar: str | None = Field(None)
    requirements: str | None = Field(None)
    whl: str | None = Field(None)
    cran: ClusterPolicyLibrariesCran | None = Field(None)
    maven: ClusterPolicyLibrariesMaven | None = Field(None)
    pypi: ClusterPolicyLibrariesPypi | None = Field(None)


class ClusterPolicyBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_cluster_policy`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    definition: str | None = Field(
        None,
        description="Policy definition: JSON document expressed in [Databricks Policy Definition Language](https://docs.databricks.com/administration-guide/clusters/policies.html#cluster-policy-definition). Cannot be used with `policy_family_id`",
    )
    description: str | None = Field(
        None, description="Additional human-readable description of the cluster policy"
    )
    max_clusters_per_user: float | None = Field(
        None,
        description="Maximum number of clusters allowed per user. When omitted, there is no limit. If specified, value must be greater than zero. * `policy_family_definition_overrides`(Optional) Policy definition JSON document expressed in Databricks Policy Definition Language. The JSON document must be passed as a string and cannot be embedded in the requests. You can use this to customize the policy definition inherited from the policy family. Policy rules specified here are merged into the inherited policy definition. * `policy_family_id` (Optional) ID of the policy family. The cluster policy's policy definition inherits the policy family's policy definition. Cannot be used with `definition`. Use `policy_family_definition_overrides` instead to customize the policy definition",
    )
    name: str | None = Field(
        None,
        description="Cluster policy name. This must be unique. Length must be between 1 and 100 characters",
    )
    policy_family_definition_overrides: str | None = Field(None)
    policy_family_id: str | None = Field(None)
    libraries: list[ClusterPolicyLibraries] | None = PluralField(
        None, plural="librariess"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_cluster_policy"
