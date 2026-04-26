from laktory.models.resources.databricks.groupmember_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.groupmember_base import GroupMemberBase


class GroupMember(GroupMemberBase):
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
