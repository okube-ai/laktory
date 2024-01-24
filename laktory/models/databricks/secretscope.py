from typing import Literal
from typing import Any
from typing import Union
from pydantic import Field
from pydantic import model_validator
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.databricks.secret import Secret
from laktory.models.databricks.secretacl import SecretAcl


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


class SecretScope(BaseModel, PulumiResource, TerraformResource):
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
    ss.to_pulumi()
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
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def core_resources(self) -> list[PulumiResource]:
        """
        - secret scope
        - secret values
        - secret scope permissions (ACL)
        """
        if self._core_resources is None:
            self._core_resources = [
                self,
            ]

            for s in self.secrets:
                self._core_resources += [
                    Secret(
                        resource_name=f"secret-{self.name}-{s.key}",
                        key=s.key,
                        value=s.value,
                        scope=f"${{resources.{self.resource_name}.id}}",
                    )
                ]

            for p in self.permissions:
                self._core_resources += [
                    SecretAcl(
                        resource_name=f"secret-scope-acl-{self.name}-{p.principal}",
                        permission=p.permission,
                        principal=p.principal,
                        scope=self.name,
                    )
                ]
                self._core_resources[-1].options.depends_on = [
                    f"${{resources.{self.resource_name}}}"
                ]

        return self._core_resources

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:SecretScope"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.SecretScope

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["permissions", "secrets"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_secret_scope"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
