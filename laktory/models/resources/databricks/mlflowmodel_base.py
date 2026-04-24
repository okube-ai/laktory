# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mlflow_model
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MlflowModelTags(BaseModel):
    key: str | None = Field(None)
    value: str | None = Field(None)


class MlflowModelBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mlflow_model`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(
        ..., description="Name of MLflow model. Change of name triggers new resource"
    )
    description: str | None = Field(
        None, description="The description of the MLflow model"
    )
    tags: list[MlflowModelTags] | None = Field(
        None, description="Tags for the MLflow model"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mlflow_model"


__all__ = ["MlflowModelTags", "MlflowModelBase"]
