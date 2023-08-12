from settus import BaseSettings


class Settings(BaseSettings):
    landing_mount_path: str = "mnt/landing/"

    # https://learn.microsoft.com/en-ca/azure/databricks/dev-tools/auth#general-host-token-and-account-id-environment-variables-and-fields
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_account_id: str = ""
    databricks_warehouse_id: str = ""


settings = Settings()
