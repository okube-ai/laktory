from laktory.models.resources.databricks.obotoken_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.obotoken_base import OboTokenBase


class OboToken(OboTokenBase):
    """
    Databricks On-Behalf-Of (OBO) Token

    Examples
    --------
    ```py
    import io

    from laktory import models

    token_yaml = '''
    application_id: baf147d1-a856-4de0-a570-8a56dbd7e234
    comment: Token for neptune service principal
    lifetime_seconds: 3600
    '''
    token = models.resources.databricks.OboToken.model_validate_yaml(
        io.StringIO(token_yaml)
    )
    ```

    References
    ----------

    * [Databricks OBO Token](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/obo_token)
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
