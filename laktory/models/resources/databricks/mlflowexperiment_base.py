# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mlflow_experiment
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MlflowExperimentTags(BaseModel):
    key: str | None = Field(None)
    value: str | None = Field(None)


class MlflowExperimentTimeouts(BaseModel):
    pass


class MlflowExperimentBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mlflow_experiment`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(
        ...,
        description="Name of MLflow experiment. It must be an absolute path within the Databricks workspace, e.g. `/Users/<some-username>/my-experiment`. For more information about changes to experiment naming conventions, see [mlflow docs](https://docs.databricks.com/applications/mlflow/experiments.html#experiment-migration)",
    )
    artifact_location: str | None = Field(
        None, description="Path to artifact location of the MLflow experiment"
    )
    creation_time: float | None = Field(None)
    description: str | None = Field(None)
    experiment_id: str | None = Field(None)
    last_update_time: float | None = Field(None)
    lifecycle_stage: str | None = Field(None)
    tags: list[MlflowExperimentTags] | None = Field(
        None, description="Tags for the MLflow experiment"
    )
    timeouts: MlflowExperimentTimeouts | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mlflow_experiment"
