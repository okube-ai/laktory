from laktory.models.resources.databricks.groupmember_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.groupmember_base import GroupMemberBase


class GroupMember(GroupMemberBase):
    """
    Databricks Group Member

    Examples
    --------
    ```py
    import io

    from laktory import models

    member_yaml = '''
    group_id: ${resources.group-role-engineers.id}
    member_id: ${resources.user-john.id}
    '''
    member = models.resources.databricks.GroupMember.model_validate_yaml(
        io.StringIO(member_yaml)
    )
    ```

    References
    ----------

    * [Databricks Group Member](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/group_member)
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
