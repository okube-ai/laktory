from typing import Literal

from pydantic import ConfigDict
from pydantic import Field
from pydantic_settings import BaseSettings

from laktory._cache import cache_dir

DEFAULT_BUILD_ROOT = cache_dir.as_posix()
DEFAULT_WORKSPACE_ROOT = "/.laktory/"
DEFAULT_RUNTIME_ROOT = "/.laktory/"


class Settings(BaseSettings):
    model_config = ConfigDict(populate_by_name=True)

    # CLI
    cli_raise_external_exceptions: bool = Field(
        False, alias="LAKTORY_CLI_RAISE_EXTERNAL_EXCEPTIONS"
    )

    # Databricks
    workspace_root: str = Field(
        DEFAULT_WORKSPACE_ROOT,
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


settings = Settings()
