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
    purge_mode: str = Field("DROP", alias="LAKTORY_PURGE_MODE")

    @field_validator("purge_mode")
    @classmethod
    def validate_purge_mode(cls, v: str) -> str:
        if v and v.upper() == "DELETE_WHERE":
            raise ValueError(
                "`purge_mode` 'DELETE_WHERE' can only be set directly on a data sink, not as "
                "a global default. A deletion predicate is inherently specific to a single "
                "sink."
            )
        if v and v.upper() == "NONE":
            raise ValueError(
                "`purge_mode` 'NONE' can only be set on a data sink, pipeline node or "
                "pipeline, not as a global default. It designates the writers of a shared "
                "sink that don't drive its purge."
            )
        return v

    # Logging
    log_level: str = Field("INFO", alias="LAKTORY_LOG_LEVEL")


settings = Settings()
