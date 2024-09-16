from pydantic import Field
from laktory.models.resources.providers.baseprovider import BaseProvider
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class DatabricksProvider(BaseProvider, PulumiResource, TerraformResource):
    """
    Databricks Provider

    Attributes
    ----------
    account_id:
        Account Id that could be found in the bottom left corner of Accounts
        Console.
    auth_type:
        Enforce specific auth type to be used in very rare cases, where a
        single provider state manages Databricks workspaces on more than one
        cloud and More than one authorization method configured error is a
        false positive. Valid values are pat, basic, azure-client-secret,
        azure-msi, azure-cli, and databricks-cli.
    azure_client_id:
        TODO
    azure_client_secret:
        TODO
    azure_environment:
        TODO
    azure_login_app_id:
        TODO
    azure_tenant_id:
        TODO
    azure_use_msi:
        TODO
    azure_workspace_resource_id:
        TODO
    client_id:
        TODO
    client_secret:
        TODO
    cluster_id:
        TODO
    config_file:
        Location of the Databricks CLI credentials file created by databricks
         configure --token command (~/.databrickscfg by default). Check
         Databricks CLI documentation for more details. The provider uses
          configuration file credentials when you don't specify
          host/token/username/password/azure attributes.
    databricks_cli_path:
        TODO
    debug_headers:
        TODO
    debug_truncate_bytes:
        TODO
    google_credentials:
        TODO
    google_service_account:
        TODO
    host:
        This is the host of the Databricks workspace. It is a URL that you use
        to login to your workspace.
    http_timeout_seconds:
        TODO
    metadata_service_url:
        TODO
    password:
        This is the user's password that can log into the workspace.
    profile:
        Connection profile specified within ~/.databrickscfg. Please check
         connection profiles section for more details.
    rate_limit:
        TODO
    retry_timeout_seconds:
        TODO
    skip_verify:
        TODO
    token:
        This is the API token to authenticate into the workspace.
    username:
        This is the username of the user that can log into the workspace.
    warehouse_id:
        TODO

    Examples
    --------
    ```py
    from laktory import models

    p = models.DatabricksProvider(
        host="adb-4623853922539974.14.azuredatabricks.net",
        token="${vars.DATABRICKS_TOKEN}",
    )
    ```
    """

    source: str = Field("databricks/databricks", exclude=True)
    version: str = Field(
        ">=1.49", exclude=True
    )  # Required to support Databricks dashboard

    account_id: str = None
    auth_type: str = None
    azure_client_id: str = None
    azure_client_secret: str = None
    azure_environment: str = None
    azure_login_app_id: str = None
    azure_tenant_id: str = None
    azure_use_msi: bool = None
    azure_workspace_resource_id: str = None
    client_id: str = None
    client_secret: str = None
    cluster_id: str = None
    config_file: str = None
    databricks_cli_path: str = None
    debug_headers: bool = None
    debug_truncate_bytes: int = None
    google_credentials: str = None
    google_service_account: str = None
    host: str = None
    http_timeout_seconds: int = None
    metadata_service_url: str = None
    password: str = None
    profile: str = None
    rate_limit: int = None
    retry_timeout_seconds: int = None
    skip_verify: bool = None
    token: str = None
    username: str = None
    warehouse_id: str = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # @property
    # def resource_key(self) -> str:
    #     return self.display_name

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "pulumi:providers:databricks"
