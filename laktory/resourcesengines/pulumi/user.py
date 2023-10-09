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
            **kwargs
    ):
        if name is None:
            name = f"user-{user.user_name}"
        super().__init__(self.t, name, {}, opts)

        kwargs["opts"] = kwargs.get("opts", pulumi.ResourceOptions())
        kwargs["opts"].parent = self
        kwargs["opts"].delete_before_replace = getattr(kwargs["opts"], "delete_before_replace", True)

        self.user = databricks.User(
            f"user-{user.user_name}",
            user_name=user.user_name,
            display_name=user.display_name,
            **kwargs,
        )

        for role in user.roles:
            databricks.UserRole(
                f"user-role-{user.user_name}-{role}",
                user_id=self.user.id,
                role=role,
                **kwargs,
            )

        if not groups:
            if user.groups:
                logger.warning(
                    "User is member of groups, but groups have not been provided. Group member resources will "
                    "be skipped."
                )
            return

        # Group Member
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

            databricks.GroupMember(
                f"group-member-{user.user_name}-{g}",
                group_id=group_id,
                member_id=self.user.id,
                **kwargs,
            )
