from typing import Union
from settus import BaseSettings
from settus import Field
from settus import SettingsConfigDict


class Settings(BaseSettings):
    # model_config = SettingsConfigDict(
    #     keyvault_url="LAKTORY_KEYVAULT_URL"
    # )

    # Configuration
    resources_engine: Union[str, None] = Field("pulumi", alias="LAKTORY_RESOURCES_ENGINE")

    # Azure
    lakehouse_sa_conn_str: Union[str, None] = Field(None, alias="LAKEHOUSE_SA_CONN_STR")

    # AWS
    aws_access_key_id: Union[str, None] = Field(None, alias="AWS_ACCESS_KEY_ID")
    aws_region: Union[str, None] = Field(None, alias="AWS_REGION")
    aws_secret_access_key: Union[str, None] = Field(None, alias="AWS_SECRET_ACCESS_KEY")

    # Databricks
    landing_mount_path: str = Field("/mnt/landing/", alias="LAKTORY_LANDING_MOUNT_PATH")

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


settings = Settings()
