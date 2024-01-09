from pydantic import Field
from pydantic import AliasChoices
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource


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


class Grants(BaseModel, PulumiResource):
    grants: list[Grant]
    catalog: str = None
    metastore: str = None
    model: str = None
    schema_: str = Field(None, validation_alias=AliasChoices("schema", "schema_"))  # required not to overwrite BaseModel attribute
    share: str = None
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
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.Grants
