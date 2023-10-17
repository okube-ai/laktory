import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.secrets.secretscope import SecretScope

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiSecretScope(PulumiResourcesEngine):

    @property
    def provider(self):
        return "databricks"

    def __init__(
            self,
            name=None,
            secret_scope: SecretScope = None,
            opts=None,
    ):
        if name is None:
            name = f"secret-scope-{secret_scope.name}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            # delete_before_replace=True,
        )

        self.secret_scope = databricks.SecretScope(
                f"secret-scope-{secret_scope.name}",
                backend_type=secret_scope.backend_type,
                keyvault_metadata=getattr(secret_scope.keyvault_metadata, "pulumi_args", None),
                name=secret_scope.name,
                opts=opts,
            )

        for s in secret_scope.secrets:
            databricks.Secret(
                f"secret-{secret_scope.name}-{s.key}",
                key=s.key,
                string_value=s.value,
                scope=self.secret_scope.id,
                opts=opts,
            )

        for p in secret_scope.permissions:
            databricks.SecretAcl(
                f"secret-scope-acl-{secret_scope.name}-{p.principal}",
                permission=p.permission,
                principal=p.principal,
                scope=self.secret_scope.name,
                opts=opts,
            )
