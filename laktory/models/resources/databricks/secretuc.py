from laktory.models.resources.databricks.secretuc_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.secretuc_base import SecretUcBase


class SecretUc(SecretUcBase):
    """
    Databricks Unity Catalog Secret

    Secret stored in a Unity Catalog schema.

    Examples
    --------
    ```py
    import io

    from laktory import models

    secret_yaml = '''
    catalog_name: dev
    schema_name: secrets
    name: api-key
    value: ${vars.api_key}
    '''
    secret = models.resources.databricks.SecretUc.model_validate_yaml(
        io.StringIO(secret_yaml)
    )
    ```

    References
    ----------

    * [Databricks Unity Catalog Secret](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/secret_uc)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.catalog_name}-{self.schema_name}-{self.name}"
