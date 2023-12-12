from typing import Literal
from typing import Any
from pydantic import Field
from pydantic import model_validator
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource
from laktory.models.databricks.secret import Secret


class SecretScopePermission(BaseModel):
    """
    Secret scope permission

    Attributes
    ----------
    permission:
        Name of the permission to assign
    principal:
        Name of the service principal to assign the permission to
    """

    permission: Literal["READ", "WRITE", "MANAGE"] = None
    principal: str = None


class SecretScopeKeyvaultMetadata(BaseModel):
    """
    Keyvault specifications when used as a secret scope backend

    Attributes
    ----------
    dns_name:

    resource_id:
        Id of the keyvault resource
    """

    dns_name: str = None
    resource_id: str = None


class SecretScope(BaseModel, BaseResource):
    """
    Databricks secret scope

    Attributes
    ----------
    backend_type:
        Backend for managing the secrets inside the scope
    keyvault_metadata:
        Keyvault specifications if used as a scope backend
    name:
        Secret scope name
    permissions:
        Permissions given to the secret scope
    secrets:
        List of secret to add to the scope

    Examples
    --------
    ```py
    from laktory import models

    ss = models.SecretScope(
        name="azure",
        secrets=[
            {"key": "keyvault-url", "value": "https://my-secrets.vault.azure.net/"},
            {"key": "client-id", "value": "f461daa2-c281-4166-bc3e-538b90223184"},
        ],
        permissions=[
            {"permission": "READ", "principal": "role-metastore-admins"},
            {"permission": "READ", "principal": "role-workspace-admins"},
        ],
    )
    ss.deploy()
    ```
    """

    backend_type: Literal["DATABRICKS", "AZURE_KEYVAULT"] = "DATABRICKS"
    keyvault_metadata: SecretScopeKeyvaultMetadata = None
    name: str = Field(...)
    permissions: list[SecretScopePermission] = []
    secrets: list[Secret] = []

    @model_validator(mode="after")
    def set_secrets_scope(self) -> Any:
        for s in self.secrets:
            s.scope = self.name

        return self

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["permissions", "secrets"]

    def deploy_with_pulumi(self, name: str = None, opts=None):
        """
        Deploy secret scope using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `{self.resource_name}`
        opts:
            Pulumi resource options

        Returns
        -------
        PulumiSecretScope:
            Pulumi group resource
        """
        from laktory.resourcesengines.pulumi.secretscope import PulumiSecretScope

        return PulumiSecretScope(name=name, secret_scope=self, opts=opts)


if __name__ == "__main__":
    from laktory import models

    ss = models.SecretScope(
        name="azure",
        secrets=[
            {"key": "keyvault-url", "value": "https://my-secrets.vault.azure.net/"},
            {"key": "client-id", "value": "f461daa2-c281-4166-bc3e-538b90223184"},
        ],
        permissions=[
            {"permission": "READ", "principal": "role-metastore-admins"},
            {"permission": "READ", "principal": "role-workspace-admins"},
        ],
    )
    print(ss)
    ss.deploy()
