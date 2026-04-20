from typing import Any
from typing import Literal
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.secret import Secret
from laktory.models.resources.databricks.secretacl import SecretAcl
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class SecretScopePermission(BaseModel):
    permission: Literal["READ", "WRITE", "MANAGE"] = Field(
        None, description="Name of the permission to assign"
    )
    principal: str = Field(
        None, description="Name of the service principal to assign the permission to"
    )


class SecretScopeKeyvaultMetadata(BaseModel):
    dns_name: str = Field(None, description="")
    resource_id: str = Field(None, description="Id of the keyvault resource")


class SecretScope(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks secret scope

    Examples
    --------
    ```py
    from laktory import models

    ss = models.resources.databricks.SecretScope(
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
    ```
    """

    backend_type: Literal["DATABRICKS", "AZURE_KEYVAULT"] = Field(
        "DATABRICKS", description="Backend for managing the secrets inside the scope"
    )
    keyvault_metadata: SecretScopeKeyvaultMetadata = Field(
        None, description="Keyvault specifications if used as a scope backend"
    )
    name: str = Field(..., description="Secret scope name")
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
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - secret values
        - secret scope permissions (ACL)
        """
        resources = []

        for s in self.secrets:
            resources += [
                Secret(
                    key=s.key,
                    value=s.value,
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
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:SecretScope"

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
