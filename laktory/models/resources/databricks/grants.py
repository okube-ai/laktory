from typing import Union
from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class Grant(BaseModel):
    """
    Grants grant

    Attributes
    ----------
    principal:
        User, group or service principal name
    privileges:
        List of allowed privileges
    """

    principal: str
    privileges: list[str]


class Grants(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Grants

    List of grants assigned to a securable object.

    Attributes
    ----------
    grants:
        List of grant assigned to the selected object
    catalog:
        Name of the catalog to assign the grants to
    external_location:
        Name of the external location to assign the grants to
    metastore:
        Name of the metastore to assign the grants to
    model
        Name of the user to assign the permission to.
    schema:
        Name of the schema to assign the permission to.
    share:
        Name of the share to assign the permission to.
    storage_credential:
        Name of the storage credential to assign the permission to.
    view:
        Name of the view to assign the permission to.
    volume:
        Name of the volume to assign the permission to.

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

    grants: list[Grant]
    catalog: str = None
    external_location: str = None
    metastore: str = None
    model: str = None
    schema_: str = Field(
        None, validation_alias=AliasChoices("schema", "schema_")
    )  # required not to overwrite BaseModel attribute
    share: str = None
    storage_credential: str = None
    table: str = None
    view: str = None
    volume: str = None

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
        return [
        ]
    
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
    

class GrantsIndividual(Grant, PulumiResource, TerraformResource):
    """
    Databricks Individual Grants

    List of grants assigned to a securable object. Does not remove existing grants, unlike `Grants`.

    Attributes
    ----------
    catalog:
        Name of the catalog to assign the grants to
    external_location:
        Name of the external location to assign the grants to
    metastore:
        Name of the metastore to assign the grants to
    model
        Name of the user to assign the permission to.
    principal:
        User, group or service principal name
    privileges:
        List of allowed privileges        
    schema:
        Name of the schema to assign the permission to.
    share:
        Name of the share to assign the permission to.
    storage_credential:
        Name of the storage credential to assign the permission to.
    view:
        Name of the view to assign the permission to.
    volume:
        Name of the volume to assign the permission to.

    Examples
    --------
    ```py
    from laktory import models

    grants = models.resources.databricks.GrantsIndividual(
        catalog="dev",
        grants=[{"principal": "metastore-admins", "privileges": ["CREATE_SCHEMA"]}],
    )
    ```
    """

    catalog: str = None
    external_location: str = None
    metastore: str = None
    model: str = None
    principal: str
    privileges: list[str]
    schema_: str = Field(
        None, validation_alias=AliasChoices("schema", "schema_")
    )  # required not to overwrite BaseModel attribute
    share: str = None
    storage_credential: str = None
    table: str = None
    view: str = None
    volume: str = None

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #
    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"schema_": "schema"}

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Grant"
    
    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
        ]
    
    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_grant"
    
    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
    