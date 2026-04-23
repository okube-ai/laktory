from laktory.models.resources.databricks.secretacl_base import SecretAclBase
from laktory.models.resources.pulumiresource import PulumiResource


class SecretAcl(SecretAclBase, PulumiResource):
    """
    Databricks secret ACL
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.scope}-{self.principal}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:SecretAcl"
