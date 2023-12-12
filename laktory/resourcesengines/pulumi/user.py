import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.databricks.user import User

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
        group_ids: dict[str, str] = None,
        opts=None,
    ):
        if name is None:
            name = user.resource_name
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        if user.id is None:
            self.user = databricks.User(
                name,
                opts=opts,
                **user.model_pulumi_dump(),
            )
            user.id = self.user.id
        else:
            self.user = databricks.User.get(
                f"user-{user.user_name}",
                id=user.id,
                opts=opts,
                **user.model_pulumi_dump(),
            )

        self.roles = []
        for role in user.roles:
            self.roles += [
                databricks.UserRole(
                    f"role-{name}-{role}",
                    user_id=self.user.id,
                    role=role,
                    opts=opts,
                )
            ]

        if not group_ids:
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
            group_id = group_ids.get(g, None)

            if group_id:
                self.group_members += [
                    databricks.GroupMember(
                        f"group-member-{user.user_name}-{g}",
                        group_id=group_id,
                        member_id=self.user.id,
                        opts=opts,
                    )
                ]
