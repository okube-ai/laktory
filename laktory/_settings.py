import os
from typing import Any
from typing import Literal

from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator
from pydantic_settings import BaseSettings

from laktory._cache import cache_dir

DEFAULT_BUILD_ROOT = cache_dir.as_posix()
DEFAULT_RUNTIME_ROOT = "/.laktory/"


class Settings(BaseSettings):
    model_config = ConfigDict(populate_by_name=True)

    # CLI
    cli_raise_external_exceptions: bool = Field(
        False, alias="LAKTORY_CLI_RAISE_EXTERNAL_EXCEPTIONS"
    )

    # Databricks
    workspace_root: str = Field(
        DEFAULT_RUNTIME_ROOT,
        alias="LAKTORY_WORKSPACE_ROOT",
    )

    # Dataframe
    dataframe_backend: str = Field("PYSPARK", alias="LAKTORY_DATAFRAME_BACKEND")
    dataframe_api: Literal["NARWHALS", "NATIVE"] = Field(
        "NARWHALS", alias="LAKTORY_DATAFRAME_API"
    )

    # Paths
    runtime_root: str = Field("", alias="LAKTORY_ROOT")
    build_root: str = Field(
        DEFAULT_BUILD_ROOT,
        alias="LAKTORY_BUILD_ROOT",
    )

    # Logging
    log_level: str = Field("INFO", alias="LAKTORY_LOG_LEVEL")

    @model_validator(mode="after")
    def update_runtime_root(self) -> Any:
        if self.runtime_root != "":
            return self

        # In Databricks
        # Could also use spark.conf.get("spark.databricks.cloudProvider") is not None
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            self.runtime_root = "/laktory/"
        else:
            # Local execution
            self.runtime_root = "./"

        return self


settings = Settings()
