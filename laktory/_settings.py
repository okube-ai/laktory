import os
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

    # Azure
    lakehouse_sa_conn_str: Union[str, None] = Field(None, alias="LAKEHOUSE_SA_CONN_STR")

    # AWS
    aws_access_key_id: Union[str, None] = Field(None, alias="AWS_ACCESS_KEY_ID")
    aws_region: Union[str, None] = Field(None, alias="AWS_REGION")
    aws_secret_access_key: Union[str, None] = Field(None, alias="AWS_SECRET_ACCESS_KEY")

    # Databricks
    workspace_env: str = Field("dev", alias="LAKTORY_WORKSPACE_ENV")
    workspace_laktory_root: str = Field(
        "/.laktory/",
        alias="LAKTORY_WORKSPACE_LAKTORY_ROOT",
    )
    workspace_landing_root: str = Field("", alias="LAKTORY_WORKSPACE_LANDING_ROOT")

    # Dataframe
    dataframe_backend: str = Field("SPARK", alias="LAKTORY_DATAFRAME_BACKEND")

    # Paths
    laktory_root: str = Field("", alias="LAKTORY_ROOT")

    # Logging
    log_level: str = Field("INFO", alias="LAKTORY_LOG_LEVEL")

    @model_validator(mode="after")
    def update_landing_root(self) -> Any:
        if self.workspace_landing_root == "":
            self.workspace_landing_root = (
                f"/Volumes/{self.workspace_env}/sources/landing/"
            )

        return self

    @model_validator(mode="after")
    def update_laktory_root(self) -> Any:

        if self.laktory_root != "":
            return self

        # In Databricks
        # Could also use spark.conf.get("spark.databricks.cloudProvider") is not None
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            self.laktory_root = "/laktory/"
        else:
            # Local execution
            self.laktory_root = "./"

        return self


settings = Settings()
