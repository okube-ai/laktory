# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_artifact_allowlist
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class ArtifactAllowlistArtifactMatcher(BaseModel):
    artifact: str = Field(..., description="The artifact path or maven coordinate")
    match_type: str = Field(
        ...,
        description="The pattern matching type of the artifact. Only `PREFIX_MATCH` is supported",
    )


class ArtifactAllowlistBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_artifact_allowlist`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    artifact_type: str = Field(
        ...,
        description="The artifact type of the allowlist. Can be `INIT_SCRIPT`, `LIBRARY_JAR` or `LIBRARY_MAVEN`. Change forces creation of a new resource",
    )
    created_at: int | None = Field(
        None, description="Time at which this artifact allowlist was set"
    )
    created_by: str | None = Field(
        None, description="Identity that set the artifact allowlist"
    )
    metastore_id: str | None = Field(None, description="ID of the parent metastore")
    artifact_matcher: list[ArtifactAllowlistArtifactMatcher] | None = PluralField(
        None, plural="artifact_matchers"
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_artifact_allowlist"


__all__ = ["ArtifactAllowlistArtifactMatcher", "ArtifactAllowlistBase"]
