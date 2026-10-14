from laktory.models.resources.databricks.permissionassignment_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.permissionassignment_base import (
    PermissionAssignmentBase,
)


class PermissionAssignment(PermissionAssignmentBase):
    """
    Databricks Permission Assignment

    Workspace-level permission assignment of an account principal (user, group or service
    principal).

    Examples
    --------
    ```py
    import io

    from laktory import models

    assignment_yaml = '''
    group_name: data-engineers
    permissions:
    - USER
    '''
    assignment = models.resources.databricks.PermissionAssignment.model_validate_yaml(
        io.StringIO(assignment_yaml)
    )
    ```

    References
    ----------

    * [Databricks Permission Assignment](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/permission_assignment)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        principal = (
            self.user_name
            or self.group_name
            or self.service_principal_name
            or self.principal_id
        )
        return str(principal)
