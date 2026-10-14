from laktory.models.resources.databricks.grouprole_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.grouprole_base import GroupRoleBase


class GroupRole(GroupRoleBase):
    """
    Databricks Group Role

    Examples
    --------
    ```py
    import io

    from laktory import models

    role_yaml = '''
    group_id: ${resources.group-admins.id}
    role: account_admin
    '''
    role = models.resources.databricks.GroupRole.model_validate_yaml(
        io.StringIO(role_yaml)
    )
    ```

    References
    ----------

    * [Databricks Group Role](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/group_role)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.role}-{self.group_id}"
