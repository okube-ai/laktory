from pydantic import Field

from laktory.models.resources.databricks.secret_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.secret_base import SecretBase


class Secret(SecretBase):
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
