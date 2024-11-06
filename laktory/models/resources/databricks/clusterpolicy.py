import json
from typing import Union
from typing import Any
from pydantic import field_validator
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class ClusterPolicyLibraryCran(BaseModel):
    package: str
    repo: str = None


class ClusterPolicyLibraryMaven(BaseModel):
    coordinates: str
    exclusions: list[str] = None
    repo: str = None


class ClusterPolicyLibraryPypi(BaseModel):
    package: str = None
    repo: str = None


class ClusterPolicyLibrary(BaseModel):
    """
    Cluster Policy Library

    Attributes
    ----------
    cran:
        Cran library specifications
    egg:
        Egg filepath
    jar:
        Jar filepath
    maven:
        TODO
    pypi:
        Pypi library specifications
    requirements:
        TODO
    whl:
        Wheel filepath
    """

    cran: ClusterPolicyLibraryCran = None
    egg: str = None
    jar: str = None
    maven: ClusterPolicyLibraryMaven = None
    pypi: ClusterPolicyLibraryPypi = None
    requirement: str = None
    whl: str = None


# class ClusterLookup(ResourceLookup):
#     """
#     Attributes
#     ----------
#     cluster_id:
#         The id of the cluster
#     """
#
#     cluster_id: str = Field(serialization_alias="id")


class ClusterPolicy(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks cluster policy

    Attributes
    ----------

    access_controls:
        List of access controls
    definition:
        Policy definition: JSON document expressed in [Databricks Policy
        Definition Language](https://docs.databricks.com/en/admin/clusters/policies.html#cluster-policy-definition).
        Cannot be used with `policy_family_id`.
    description:
        Additional human-readable description of the cluster policy.
    libraries:
        TODO
    max_clusters_per_user:
        Maximum number of clusters allowed per user. When omitted, there is no
        limit. If specified, value must be greater than zero.
    name:
        Cluster policy name. This must be unique. Length must be between 1 and
        100 characters.
    policy_family_definition_overrides:
        Policy definition JSON document expressed in Databricks Policy
        Definition Language. The JSON document must be passed as a string and
         cannot be embedded in the requests. You can use this to customize the
         policy definition inherited from the policy family. Policy rules
         specified here are merged into the inherited policy definition.
    policy_family_id:
        ID of the policy family. The cluster policy's policy definition
        inherits the policy family's policy definition. Cannot be used with
        `definition`. Use `policy_family_definition_overrides` instead to
        customize the policy definition.

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
        access_controls=[{"permission_level": "CAN_USE", "group_name": "account users"}],
    )
    ```

    References
    ----------

    * [Databricks Cluster Policy](https://docs.databricks.com/api/workspace/clusterpolicies/create)
    * [Pulumi Databricks Cluster Policy](https://www.pulumi.com/registry/packages/databricks/api-docs/clusterpolicy)

    """

    access_controls: list[AccessControl] = []
    definition: Union[str, dict[str, Any]] = None
    description: str = None
    libraries: list[ClusterPolicyLibrary] = None
    max_clusters_per_user: int = None
    name: str
    policy_family_definition_overrides: str = None
    policy_family_id: str = None

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
