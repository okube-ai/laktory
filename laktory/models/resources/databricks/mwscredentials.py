from laktory.models.resources.databricks.mwscredentials_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwscredentials_base import MwsCredentialsBase


class MwsCredentials(MwsCredentialsBase):
    """
    Databricks Mws Credentials

    Examples
    --------
    ```py
    import io

    from laktory import models

    credentials_yaml = '''
    credentials_name: my-credentials
    role_arn: arn:aws:iam::000000000000:role/my-cross-account-role
    '''
    credentials = models.resources.databricks.MwsCredentials.model_validate_yaml(
        io.StringIO(credentials_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS Credentials](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_credentials)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.credentials_name
