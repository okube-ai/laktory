from typing import Literal
from typing import Any
from pydantic import Field
from pydantic import model_validator
from laktory.models.base import BaseModel
from laktory.models.resources import Resources
from laktory.models.secrets.secret import Secret


class SecretScopePermission(BaseModel):
    permission: Literal["READ", "WRITE", "MANAGE"] = None
    principal: str = None
    scope: str = None


class SecretScopeKeyvaultMetadata(BaseModel):
    dns_name: str = None
    resource_id: str = None

    @property
    def pulumi_args(self):
        import pulumi_databricks as databricks
        return databricks.SecretScopeKeyvaultMetadataArgs(**self.model_dump())


class SecretScope(BaseModel, Resources):
    backend_type: Literal["DATABRICKS", "AZURE_KEYVAULT"] = "DATABRICKS"
    keyvault_metadata: SecretScopeKeyvaultMetadata = None
    name: str = Field(...)
    secrets: list[Secret] = []
    permissions: list[SecretScopePermission] = []

    @model_validator(mode="after")
    def set_secrets_scope(self) -> Any:

        for s in self.secrets:
            s.scope = self.name

        return self

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        from laktory.resourcesengines.pulumi.secretscope import PulumiSecretScope
        return PulumiSecretScope(name=name, secret_scope=self, opts=opts)
