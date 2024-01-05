from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource


class GroupMember(BaseModel, PulumiResource):
    """
    Databricks secret ACL

    Attributes
    ----------
    group_id:
        This is the id of the group resource.
    member_id:
        This is the id of the group, service principal, or user.
    """

    group_id: str = None
    member_id: str = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:GroupMember"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.GroupMember
