from typing import Any
from typing import Literal
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.secret import Secret
from laktory.models.resources.databricks.secretacl import SecretAcl
from laktory.models.resources.databricks.secretscope_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.secretscope_base import SecretScopeBase


class SecretScopePermission(BaseModel):
    permission: Literal["READ", "WRITE", "MANAGE"] = Field(
        None, description="Name of the permission to assign"
    )
    principal: str = Field(
        None, description="Name of the service principal to assign the permission to"
    )


class SecretScope(SecretScopeBase):
    """
    Databricks secret scope

    Examples
    --------
    ```py
    import io

    from laktory import models

    scope_yaml = '''
    name: azure
    secrets:
    - key: keyvault-url
      string_value: https://my-secrets.vault.azure.net/
    - key: client-id
      string_value: f461daa2-c281-4166-bc3e-538b90223184
    permissions:
    - permission: READ
      principal: role-metastore-admins
    - permission: READ
      principal: role-workspace-admins
    '''
    scope = models.resources.databricks.SecretScope.model_validate_yaml(
        io.StringIO(scope_yaml)
    )
    ```

    References
    ----------

    * [Databricks Secret Scope](https://docs.databricks.com/en/security/secrets/secret-scopes.html)
    """

    permissions: list[SecretScopePermission] = Field(
        [], description="Permissions given to the secret scope"
    )
    secrets: list[Secret] = Field([], description="List of secret to add to the scope")

    @model_validator(mode="after")
    def set_secrets_scope(self) -> Any:
        for s in self.secrets:
            s.scope = self.name

        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
        """
        - secret values
        - secret scope permissions (ACL)
        """
        resources = []

        for s in self.secrets:
            resources += [
                Secret(
                    key=s.key,
                    string_value=s.string_value,
                    scope=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        for p in self.permissions:
            resources += [
                SecretAcl(
                    resource_name=f"secret-scope-acl-{self.name}-{p.principal}",
                    permission=p.permission,
                    principal=p.principal,
                    scope=self.name,
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["permissions", "secrets"]
