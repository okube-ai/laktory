# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mlflow_experiment
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MlflowExperimentTags(BaseModel):
    key: str | None = Field(None)
    value: str | None = Field(None)


class MlflowExperimentTimeouts(BaseModel):
    pass


class MlflowExperimentTraceLocationUcTraceLocation(BaseModel):
    catalog: str = Field(..., description="Name of the Unity Catalog catalog")
    effective_table_prefix: str | None = Field(
        None,
        description="(Computed) The trace-table prefix actually in effect: `table_prefix` if it was set on creation, otherwise the server-generated default",
    )
    schema_: str = Field(
        ...,
        description="Name of the Unity Catalog schema within `catalog`",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )
    table_prefix: str | None = Field(
        None,
        description="Prefix for the generated trace tables (named `{catalog}.{schema}.{table_prefix}_otel_*`). If omitted, the server generates a default prefix derived from the experiment ID; the field then stays empty and the resolved value is available in `effective_table_prefix`",
    )


class MlflowExperimentTraceLocation(BaseModel):
    uc_trace_location: MlflowExperimentTraceLocationUcTraceLocation | None = Field(
        None,
        description="The Unity Catalog storage location. This block consists of the following fields:",
    )


class MlflowExperimentBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mlflow_experiment`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(
        ...,
        description="Name of MLflow experiment. It must be an absolute path within the Databricks workspace, e.g. `/Users/<some-username>/my-experiment`. For more information about changes to experiment naming conventions, see [mlflow docs](https://docs.databricks.com/applications/mlflow/experiments.html#experiment-migration)",
    )
    artifact_location: str | None = Field(
        None, description="Path to artifact location of the MLflow experiment"
    )
    creation_time: int | None = Field(None)
    description: str | None = Field(None)
    experiment_id: str | None = Field(None)
    last_update_time: int | None = Field(None)
    lifecycle_stage: str | None = Field(None)
    tags: list[MlflowExperimentTags] | None = Field(
        None, description="Tags for the MLflow experiment"
    )
    timeouts: MlflowExperimentTimeouts | None = Field(None)
    trace_location: MlflowExperimentTraceLocation | None = Field(
        None,
        description="Unity Catalog location where the experiment's traces are stored. Cannot be changed after the experiment is created; changing it forces replacement of the experiment. This block consists of the following fields:",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mlflow_experiment"


__all__ = [
    "MlflowExperimentBase",
    "MlflowExperimentTags",
    "MlflowExperimentTimeouts",
    "MlflowExperimentTraceLocation",
    "MlflowExperimentTraceLocationUcTraceLocation",
]
