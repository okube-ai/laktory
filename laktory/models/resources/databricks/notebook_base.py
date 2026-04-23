# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_notebook
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class NotebookBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_notebook`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    content_base64: str | None = Field(
        None,
        description="The base64-encoded notebook source code. Conflicts with `source`. Use of `content_base64` is discouraged, as it's increasing memory footprint of Terraform state and should only be used in exceptional circumstances, like creating a notebook with configuration properties for a data pipeline",
    )
    format: str | None = Field(None)
    language: str | None = Field(
        None,
        description="(required with `content_base64`) One of `SCALA`, `PYTHON`, `SQL`, `R`",
    )
    md5: str | None = Field(None)
    object_id: float | None = Field(
        None, description="Unique identifier for a NOTEBOOK"
    )
    object_type: str | None = Field(None)
    source: str | None = Field(
        None,
        description="Path to notebook in source code format on local filesystem. Conflicts with `content_base64`",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_notebook"


__all__ = ["NotebookBase"]
