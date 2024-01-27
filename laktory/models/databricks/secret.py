from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class Secret(BaseModel, PulumiResource, TerraformResource):
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

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_secret"

    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames
