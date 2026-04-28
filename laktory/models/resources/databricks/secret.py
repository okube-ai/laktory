from pydantic import Field

from laktory.models.resources.databricks.secret_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.secret_base import SecretBase


class Secret(SecretBase):
    """
    Databricks secret

    Examples
    --------
    ```py
    import io

    from laktory import models

    secret_yaml = '''
    scope: azure
    key: client-id
    string_value: f461daa2-c281-4166-bc3e-538b90223184
    '''
    secret = models.resources.databricks.Secret.model_validate_yaml(
        io.StringIO(secret_yaml)
    )
    ```

    References
    ----------

    * [Databricks Secrets](https://docs.databricks.com/en/security/secrets/index.html)
    """

    scope: str | None = Field(None, description="Name of the secret scope")

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.scope}-{self.key}"
