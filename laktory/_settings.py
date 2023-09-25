from typing import Union
from settus import BaseSettings
from settus import Field


class Settings(BaseSettings):

    # Storage
    lakehouse_sa_conn_str: Union[str, None] = Field(None, alias="LAKEHOUSE_SA_CONN_STR")
    landing_mount_path: str = Field("/mnt/landing/", alias="LAKTORY_LANDING_MOUNT_PATH")

    # Logging
    log_level: str = Field("INFO", alias="LAKTORY_LOG_LEVEL")

    # https://learn.microsoft.com/en-ca/azure/databricks/dev-tools/auth#general-host-token-and-account-id-environment-variables-and-fields
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_account_id: str = ""
    databricks_warehouse_id: str = ""


settings = Settings()
