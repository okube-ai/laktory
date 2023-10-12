from typing import Union
import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.group import Group
from laktory.models.user import User

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiUser(PulumiResourcesEngine):

    @property
    def provider(self):
        return "databricks"

    def __init__(
            self,
            name=None,
            user: User = None,
            groups: Union[list[Group], dict] = None,
            opts=None,
    ):
        if name is None:
            name = f"user-{user.user_name}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        self.user = databricks.User(
            f"user-{user.user_name}",
            user_name=user.user_name,
            display_name=user.display_name,
            disable_as_user_deletion=user.disable_as_user_deletion,
            opts=opts,
        )

        self.roles = []
        for role in user.roles:
            self.roles += [databricks.UserRole(
                f"user-role-{user.user_name}-{role}",
                user_id=self.user.id,
                role=role,
                opts=opts,
            )]

        if not groups:
            if user.groups:
                logger.warning(
                    "User is member of groups, but groups have not been provided. Group member resources will "
                    "be skipped."
                )
            return

        # Group Member
        self.group_members = []
        for g in user.groups:

            # Find matching group
            group_id = None

            # List of Group models
            if isinstance(groups, list):
                for _g in groups:
                    if g == _g.display_name:
                        group_id = _g.resources.group.id

            elif isinstance(groups, dict):
                group_id = groups[g]

            self.group_members += [databricks.GroupMember(
                f"group-member-{user.user_name}-{g}",
                group_id=group_id,
                member_id=self.user.id,
                opts=opts,
            )]
