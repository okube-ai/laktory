from pydantic import Field

from laktory.models.resources.databricks.secret_base import SecretBase
from laktory.models.resources.pulumiresource import PulumiResource


class Secret(SecretBase, PulumiResource):
    """
    Databricks secret
    """

    scope: str | None = Field(None, description="Name of the secret scope")

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.scope}-{self.key}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Secret"
