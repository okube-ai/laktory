from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

from pydantic import Field

from laktory.models.resources.providers.baseprovider import BaseProvider
from laktory.models.resources.terraformresource import TerraformResource

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


class DatabricksProvider(BaseProvider, TerraformResource):
    """
    Databricks Provider

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

    account_id: str = Field(
        None,
        description="Account Id that could be found in the bottom left corner of Accounts Console.",
    )
    auth_type: str = Field(
        None,
        description="""
    Enforce specific auth type to be used in very rare cases, where a single provider state manages Databricks 
    workspaces on more than one cloud and More than one authorization method configured error is a
    false positive. Valid values are pat, basic, azure-client-secret, azure-msi, azure-cli, and databricks-cli.
    """,
    )
    azure_client_id: str = Field(None, description="")
    azure_client_secret: str = Field(None, description="")
    azure_environment: str = Field(None, description="")
    azure_login_app_id: str = Field(None, description="")
    azure_tenant_id: str = Field(None, description="")
    azure_use_msi: bool = Field(None, description="")
    azure_workspace_resource_id: str = Field(None, description="")
    client_id: str = Field(None, description="")
    client_secret: str = Field(None, description="")
    cluster_id: str = Field(None, description="")
    config_file: str = Field(
        None,
        description="""
    Location of the Databricks CLI credentials file created by databricks configure 
    --token command (~/.databrickscfg by default). Check Databricks CLI documentation for more details. The provider
     uses configuration file credentials when you don't specify host/token/username/password/azure attributes.
    """,
    )
    databricks_cli_path: str = Field(None, description="")
    debug_headers: bool = Field(None, description="")
    debug_truncate_bytes: int = Field(None, description="")
    google_credentials: str = Field(None, description="")
    google_service_account: str = Field(None, description="")
    host: str = Field(
        None,
        description="This is the host of the Databricks workspace. It is a URL that you use to login to your workspace.",
    )
    http_timeout_seconds: int = Field(None, description="")
    metadata_service_url: str = Field(None, description="")
    password: str = Field(
        None, description="This is the user's password that can log into the workspace."
    )
    profile: str = Field(
        None,
        description="Connection profile specified within ~/.databrickscfg. Please check connection profiles section for more details.",
    )
    rate_limit: int = Field(None, description="")
    retry_timeout_seconds: int = Field(None, description="")
    skip_verify: bool = Field(None, description="")
    token: str = Field(
        None, description="This is the API token to authenticate into the workspace."
    )
    username: str = Field(
        None,
        description="This is the username of the user that can log into the workspace.",
    )
    warehouse_id: str = Field(None, description="")

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    def workspace_client_kwargs(self) -> dict[str, Any]:
        """
        Return kwargs dict suitable for instantiating a Databricks SDK
        ``WorkspaceClient``. Only fields that are explicitly set are included.
        """
        keys = (
            "host",
            "account_id",
            "auth_type",
            "azure_client_id",
            "azure_client_secret",
            "azure_environment",
            "azure_tenant_id",
            "azure_workspace_resource_id",
            "client_id",
            "client_secret",
            "cluster_id",
            "config_file",
            "debug_headers",
            "debug_truncate_bytes",
            "google_credentials",
            "google_service_account",
            "password",
            "profile",
            "token",
            "username",
        )
        return {k: v for k in keys if (v := getattr(self, k, None)) is not None}

    @property
    def workspace_client(self) -> "WorkspaceClient":
        """Databricks SDK WorkspaceClient built from this provider's credentials."""
        from databricks.sdk import WorkspaceClient

        from laktory._useragent import DATABRICKS_USER_AGENT
        from laktory._useragent import VERSION

        wc = WorkspaceClient(**self.workspace_client_kwargs())
        wc.config.with_user_agent_extra(key=DATABRICKS_USER_AGENT, value=VERSION)
        return wc
