import json
from typing import Any
from typing import Union

from pydantic import Field
from pydantic import field_validator

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class ClusterPolicyLibraryCran(BaseModel):
    package: str = Field(..., description="")
    repo: str = Field(None, description="")


class ClusterPolicyLibraryMaven(BaseModel):
    coordinates: str = Field(..., description="")
    exclusions: list[str] = Field(None, description="")
    repo: str = Field(None, description="")


class ClusterPolicyLibraryPypi(BaseModel):
    package: str = Field(None, description="")
    repo: str = Field(None, description="")


class ClusterPolicyLibrary(BaseModel):
    cran: ClusterPolicyLibraryCran = Field(
        None, description="Cran library specifications"
    )
    egg: str = Field(None, description="Egg filepath")
    jar: str = Field(None, description="Jar filepath")
    maven: ClusterPolicyLibraryMaven = Field(None, description="")
    pypi: ClusterPolicyLibraryPypi = Field(
        None, description="Pypi library specifications"
    )
    requirement: str = Field(None, description="")
    whl: str = Field(None, description="Wheel filepath")


class ClusterPolicy(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks cluster policy

    Examples
    --------
    ```py
    from laktory import models

    cluster = models.resources.databricks.ClusterPolicy(
        name="okube",
        definition={
            "dbus_per_hour": {
                "type": "range",
                "maxValue": 10,
            },
            "autotermination_minutes": {"type": "fixed", "value": 30, "hidden": True},
            "custom_tags.team": {
                "type": "fixed",
                "value": "okube",
            },
        },
        libraries=[
            {
                "pypi": {
                    "package": "laktory==0.5.0",
                }
            }
        ],
        access_controls=[
            {"permission_level": "CAN_USE", "group_name": "account users"}
        ],
    )
    ```

    References
    ----------

    * [Databricks Cluster Policy](https://docs.databricks.com/api/workspace/clusterpolicies/create)
    * [Pulumi Databricks Cluster Policy](https://www.pulumi.com/registry/packages/databricks/api-docs/clusterpolicy)

    """

    access_controls: list[AccessControl] = Field(
        [], description="List of access controls"
    )
    definition: Union[str, dict[str, Any]] = Field(
        None,
        description="""
    Policy definition: JSON document expressed in [Databricks Policy Definition Language](https://docs.databricks.com/en/admin/clusters/policies.html#cluster-policy-definition).
    Cannot be used with `policy_family_id`.
    """,
    )
    description: str = Field(
        None, description="Additional human-readable description of the cluster policy."
    )
    libraries: list[ClusterPolicyLibrary] = Field(None, description="")
    max_clusters_per_user: int = Field(
        None,
        description="""
    Maximum number of clusters allowed per user. When omitted, there is no limit. If specified, value must be greater 
    than zero.
    """,
    )
    name: str = Field(
        ...,
        description="Cluster policy name. This must be unique. Length must be between 1 and 100 characters.",
    )
    policy_family_definition_overrides: str = Field(
        None,
        description="""
    Policy definition JSON document expressed in Databricks Policy Definition Language. The JSON document must be 
    passed as a string and cannot be embedded in the requests. You can use this to customize the policy definition 
    inherited from the policy family. Policy rules specified here are merged into the inherited policy definition.
    """,
    )
    policy_family_id: str = Field(
        None,
        description="""
    ID of the policy family. The cluster policy's policy definition inherits the policy family's policy definition.
    Cannot be used with `definition`. Use `policy_family_definition_overrides` instead to customize the policy 
    definition.
    """,
    )

    @field_validator("definition")
    def validate_type(cls, v: Union[str, dict[str, str]]) -> str:
        if isinstance(v, dict):
            v = json.dumps(v)
        return v

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    cluster_policy_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:ClusterPolicy"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "libraries": "libraries",
        }

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_cluster_policy"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
