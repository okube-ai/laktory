from laktory.models.resources.databricks.customappintegration_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.customappintegration_base import (
    CustomAppIntegrationBase,
)


class CustomAppIntegration(CustomAppIntegrationBase):
    """
    Databricks Custom App Integration

    Custom OAuth application integration.

    Examples
    --------
    ```py
    import io

    from laktory import models

    integration_yaml = '''
    name: my-app
    confidential: true
    redirect_urls:
    - https://example.com/callback
    scopes:
    - all-apis
    '''
    integration = models.resources.databricks.CustomAppIntegration.model_validate_yaml(
        io.StringIO(integration_yaml)
    )
    ```

    References
    ----------

    * [Databricks Custom App Integration](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/custom_app_integration)
    """
