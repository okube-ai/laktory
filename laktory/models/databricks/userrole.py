from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource


class UserRole(BaseModel, PulumiResource):
    """
    Databricks User role

    Attributes
    ----------
    role:
        This is the id of the role or instance profile resource.
    user_id:
        This is the id of the user resource.
    """

    role: str = None
    user_id: str = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:UserRole"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.UserRole
