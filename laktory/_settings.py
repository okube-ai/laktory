from typing import Literal

from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic_settings import BaseSettings

from laktory._cache import cache_dir

DEFAULT_BUILD_ROOT = cache_dir.as_posix()
DEFAULT_WORKSPACE_ROOT = "/.laktory/"
DEFAULT_RUNTIME_ROOT = "./.laktory/"


class Settings(BaseSettings):
    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

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
    runtime_root: str = Field(DEFAULT_RUNTIME_ROOT, alias="LAKTORY_RUNTIME_ROOT")
    build_root: str = Field(
        DEFAULT_BUILD_ROOT,
        alias="LAKTORY_BUILD_ROOT",
    )

    # Narwhals extensions
    register_nw_extensions: bool = Field(True, alias="LAKTORY_REGISTER_NW_EXTENSIONS")

    # Pipeline
    full_refresh_mode: str = Field("DROP", alias="LAKTORY_FULL_REFRESH_MODE")

    @field_validator("full_refresh_mode")
    @classmethod
    def validate_full_refresh_mode(cls, v: str) -> str:
        if v and v.upper() == "DELETE_WHERE":
            raise ValueError(
                "`full_refresh_mode` 'DELETE_WHERE' can only be set directly on a data sink, not as "
                "a global default. A deletion predicate is inherently specific to a single "
                "sink."
            )
        if v and v.upper() not in ["DROP", "TRUNCATE"]:
            raise ValueError(
                f"`full_refresh_mode` '{v}' is not supported as a global default. Use 'DROP' or "
                "'TRUNCATE'."
            )
        return v

    # Logging
    log_level: str = Field("INFO", alias="LAKTORY_LOG_LEVEL")


settings = Settings()
