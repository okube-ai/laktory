from laktory.models.resources.databricks.obotoken_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.obotoken_base import OboTokenBase


class OboToken(OboTokenBase):
    """
    Databricks On-Behalf-Of token
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.application_id

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
