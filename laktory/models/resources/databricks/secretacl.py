from laktory.models.resources.databricks.secretacl_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.secretacl_base import SecretAclBase


class SecretAcl(SecretAclBase):
    """
    Databricks secret ACL
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.scope}-{self.principal}"
