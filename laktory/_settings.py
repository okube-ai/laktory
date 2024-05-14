from pydantic import model_validator
from typing import Union
from typing import Any
from settus import BaseSettings
from settus import Field
from settus import SettingsConfigDict


class Settings(BaseSettings):
    # model_config = SettingsConfigDict(
    #     keyvault_url="LAKTORY_KEYVAULT_URL"
    # )

    # CLI
    cli_raise_external_exceptions: bool = Field(
        False, alias="LAKTORY_CLI_RAISE_EXTERNAL_EXCEPTIONS"
    )

    # Configuration
    iac_backend: Union[str, None] = Field("pulumi", alias="LAKTORY_IAC_BACKEND")

    # Models
    camel_serialization: bool = Field(False)
    singular_serialization: bool = Field(False)

    # Azure
    lakehouse_sa_conn_str: Union[str, None] = Field(None, alias="LAKEHOUSE_SA_CONN_STR")

    # AWS
    aws_access_key_id: Union[str, None] = Field(None, alias="AWS_ACCESS_KEY_ID")
    aws_region: Union[str, None] = Field(None, alias="AWS_REGION")
    aws_secret_access_key: Union[str, None] = Field(None, alias="AWS_SECRET_ACCESS_KEY")

    # Databricks
    workspace_env: str = Field("dev", alias="LAKTORY_WORKSPACE_ENV")
    workspace_laktory_root: str = Field(
        "/.laktory/", alias="LAKTORY_WORKSPACE_LAKTORY_ROOT"
    )  # TODO: Review variable name
    workspace_landing_root: str = Field("", alias="LAKTORY_WORKSPACE_LANDING_ROOT")

    # Logging
    log_level: str = Field("INFO", alias="LAKTORY_LOG_LEVEL")

    # https://learn.microsoft.com/en-ca/azure/databricks/dev-tools/auth#general-host-token-and-account-id-environment-variables-and-fields
    # databricks_host: Union[str, None] = Field(None, alias="databricks_host")
    # databricks_token: Union[str, None] = Field(None, alias="databricks_token")
    # databricks_account_id: Union[str, None] = Field(None, alias="databricks_account_id")
    # databricks_warehouse_id: Union[str, None] = Field(None, alias="databricks_warehouse_id")
    databricks_host: Union[str, None] = Field(None)
    databricks_token: Union[str, None] = Field(None)
    databricks_account_id: Union[str, None] = Field(None)
    databricks_warehouse_id: Union[str, None] = Field(None)

    @model_validator(mode="after")
    def update_landing_root(self) -> Any:
        if self.workspace_landing_root == "":
            self.workspace_landing_root = (
                f"/Volumes/{self.workspace_env}/sources/landing/"
            )

        return self


settings = Settings()
