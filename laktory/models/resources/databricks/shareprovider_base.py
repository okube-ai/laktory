# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_provider
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class ShareProviderBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_provider`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    authentication_type: str = Field(
        ...,
        description="The delta sharing authentication type. Valid values are `TOKEN`",
    )
    name: str = Field(
        ..., description="Name of provider. Change forces creation of a new resource"
    )
    recipient_profile_str: str = Field(
        ..., description="This is the json file that is created from a recipient url"
    )
    comment: str | None = Field(None, description="Description about the provider")

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_provider"


__all__ = ["ShareProviderBase"]
