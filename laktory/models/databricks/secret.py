from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource


class Secret(BaseModel, PulumiResource):
    """
    Databricks secret

    Attributes
    ----------
    scope:
        Scope associated with the secret
    key:
        Key associated with the secret.
    value:
        Value associated with the secret
    """

    scope: str = None
    key: str = None
    value: str = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Secret"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.Secret

    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"value": "string_value"}
