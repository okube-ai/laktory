from pydantic import Field

from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.instancepool_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.instancepool_base import InstancePoolBase
from laktory.models.resources.databricks.permissions import Permissions


class InstancePool(InstancePoolBase):
    """
    Databricks instance pool

    Examples
    --------
    ```py
    import io

    from laktory import models

    instance_pool_yaml = '''
    instance_pool_name: default
    node_type_id: Standard_DS3_v2
    min_idle_instances: 0
    idle_instance_autotermination_minutes: 10
    access_controls:
    - group_name: role-engineers
      permission_level: CAN_ATTACH_TO
    '''
    instance_pool = models.resources.databricks.InstancePool.model_validate_yaml(
        io.StringIO(instance_pool_yaml)
    )
    ```

    References
    ----------

    * [Databricks Instance Pool](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/instance_pool)
    """

    access_controls: list[AccessControl] = Field(
        [], description="List of access controls"
    )

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
                    resource_options={"name": f"permissions-{self.resource_name}"},
                    access_controls=self.access_controls,
                    instance_pool_id=f"${{resources.{self.resource_name}.instance_pool_id}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["access_controls"]
