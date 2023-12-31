from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource


class SecretAcl(BaseModel, PulumiResource):
    """
    Databricks secret ACL

    Attributes
    ----------
    permission:
        Scope associated with the secret
    principal:
        Key associated with the secret.
    scope:
        Value associated with the secret
    """

    permission: str = None
    principal: str = None
    scope: str = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:SecretAcl"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.SecretAcl
