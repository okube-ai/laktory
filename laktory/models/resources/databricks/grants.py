from typing import Union

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class GrantsGrant(BaseModel):
    """
    Grants grant
    """

    principal: str = Field(..., description="User, group or service principal name")
    privileges: list[str] = Field(..., description="List of allowed privileges")


class Grants(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Grants

    Authoritative for all principals. Sets the grants of a securable and replaces any
    existing grants defined inside or outside of Laktory.

    Examples
    --------
    ```py
    from laktory import models

    grants = models.resources.databricks.Grants(
        catalog="dev",
        grants=[{"principal": "metastore-admins", "privileges": ["CREATE_SCHEMA"]}],
    )
    ```
    """

    catalog: str = Field(
        None, description="Name of the catalog to assign the grants to"
    )
    external_location: str = Field(
        None, description="Name of the external location to assign the grants to"
    )
    grants: list[GrantsGrant] = Field(
        ..., description="List of grant assigned to the selected object"
    )
    metastore: str = Field(
        None, description="Name of the metastore to assign the grants to"
    )
    model: str = Field(
        None, description="Name of the user to assign the permission to."
    )
    schema_: str = Field(
        None,
        validation_alias=AliasChoices("schema", "schema_"),
        description="Name of the schema to assign the permission to.",
    )  # required not to overwrite BaseModel attribute
    share: str = Field(
        None, description="Name of the share to assign the permission to."
    )
    storage_credential: str = Field(
        None, description="Name of the storage credential to assign the permission to."
    )
    table: str = Field(
        None, description="Name of the table to assign the permission to."
    )
    view: str = Field(None, description="Name of the view to assign the permission to.")
    volume: str = Field(
        None, description="Name of the volume to assign the permission to."
    )

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #
    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"schema_": "schema"}

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Grants"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return []

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_grants"

    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
