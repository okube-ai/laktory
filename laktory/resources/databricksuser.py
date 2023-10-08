from typing import Union
import pulumi_databricks as databricks
from laktory.resources.basecomponentresource import BaseComponentResource
from laktory.models.group import Group

from laktory._logger import get_logger

logger = get_logger(__name__)


class DatabricksUser(BaseComponentResource):

    @property
    def provider(self):
        return "databricks"

    @property
    def default_name(self) -> str:
        return f"user-{self.model.user_name}"

    def set_resources(self, groups: Union[list[Group], dict] = None, **kwargs):
        kwargs["opts"].delete_before_replace = getattr(kwargs["opts"], "delete_before_replace", True)
        self.user = databricks.User(
            f"user-{self.model.user_name}",
            user_name=self.model.user_name,
            display_name=self.model.display_name,
            **kwargs,
        )

        for role in self.model.roles:
            databricks.UserRole(
                f"user-role-{self.model.user_name}-{role}",
                user_id=self.user.id,
                role=role,
                **kwargs,
            )

        if not groups:
            if self.model.groups:
                logger.warning(
                    "User is member of groups, but groups have not been provided. Group member resources will "
                    "be skipped."
                )
            return

        # Group Member
        for g in self.model.groups:

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
                f"group-member-{self.model.user_name}-{g}",
                group_id=group_id,
                member_id=self.user.id,
                **kwargs,
            )
