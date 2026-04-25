import json
from typing import Any
from typing import Union

from pydantic import Field
from pydantic import field_validator

from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.clusterpolicy_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.clusterpolicy_base import ClusterPolicyBase
from laktory.models.resources.databricks.permissions import Permissions


class ClusterPolicy(ClusterPolicyBase):
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

    """

    access_controls: list[AccessControl] = Field(
        [], description="List of access controls"
    )
    definition: Union[str, dict[str, Any]] = Field(
        None,
        description="Policy definition: JSON document expressed in Databricks Policy Definition Language.",
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
    def additional_core_resources(self) -> list:
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]
