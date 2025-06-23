import os
from typing import Any
from typing import Union

from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = ConfigDict(populate_by_name=True)

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
