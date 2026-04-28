from laktory.models.resources.databricks.secretacl_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.secretacl_base import SecretAclBase


class SecretAcl(SecretAclBase):
    """
    Databricks Secret ACL

    Examples
    --------
    ```py
    import io

    from laktory import models

    acl_yaml = '''
    scope: azure
    principal: role-metastore-admins
    permission: READ
    '''
    acl = models.resources.databricks.SecretAcl.model_validate_yaml(
        io.StringIO(acl_yaml)
    )
    ```

    References
    ----------

    * [Databricks Secret ACL](https://docs.databricks.com/en/security/secrets/secret-acl.html)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.scope}-{self.principal}"
