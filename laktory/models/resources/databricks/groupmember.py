from laktory.models.resources.databricks.groupmember_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.groupmember_base import GroupMemberBase
from laktory.models.resources.pulumiresource import PulumiResource


class GroupMember(GroupMemberBase, PulumiResource):
    """
    Databricks secret ACL
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.group_id}-{self.member_id}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:GroupMember"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
