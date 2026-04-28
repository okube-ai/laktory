from laktory.models.resources.databricks.userrole_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.userrole_base import UserRoleBase


class UserRole(UserRoleBase):
    """
    Databricks User Role

    Examples
    --------
    ```py
    import io

    from laktory import models

    role_yaml = '''
    user_id: ${resources.user-john.id}
    role: account_admin
    '''
    role = models.resources.databricks.UserRole.model_validate_yaml(
        io.StringIO(role_yaml)
    )
    ```

    References
    ----------

    * [Databricks User Role](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/user_role)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.role}-{self.user_id}"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
