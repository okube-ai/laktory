from laktory.models.basemodel import BaseModel
from laktory.models.databricks.permission import Permission
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
    schema: str = None
    share: str = None
    view: str = None
    volume: str = None

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Grants"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks
        return databricks.Grants

    @property
    def all_resources(self) -> list[PulumiResource]:
        return [self]
