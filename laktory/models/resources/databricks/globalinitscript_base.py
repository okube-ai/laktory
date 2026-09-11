# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_global_init_script
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class GlobalInitScriptTimeouts(BaseModel):
    pass


class GlobalInitScriptBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_global_init_script`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(..., description="the name of the script.  It should be unique")
    content_base64: str | None = Field(
        None,
        description="The base64-encoded source code global init script. Conflicts with `source`. Use of `content_base64` is discouraged, as it's increasing memory footprint of Terraform state and should only be used in exceptional circumstances",
    )
    enabled: bool | None = Field(
        None, description="specifies if the script is enabled for execution, or not"
    )
    md5: str | None = Field(None)
    position: int | None = Field(
        None,
        description="the position of a global init script, where `0` represents the first global init script to run, `1` is the second global init script to run, and so on. When omitted, the script gets the last position",
    )
    source: str | None = Field(
        None,
        description="Path to script's source code on local filesystem. Conflicts with `content_base64`",
    )
    timeouts: GlobalInitScriptTimeouts | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_global_init_script"


__all__ = ["GlobalInitScriptBase", "GlobalInitScriptTimeouts"]
