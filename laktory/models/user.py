from laktory.models.base import BaseModel
from laktory.models.resources import Resources


class User(BaseModel, Resources):
    user_name: str
    display_name: str = None
    workspace_access: bool = None
    groups: list[str] = []
    roles: list[str] = []

    # Deployment options
    disable_as_user_deletion: bool = False

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #
    @property
    def pulumi_excludes(self) -> list[str]:
        return ["groups", "roles"]

    def deploy_with_pulumi(self, name=None, group_ids=None, opts=None):
        from laktory.resourcesengines.pulumi.user import PulumiUser

        return PulumiUser(name=name, user=self, group_ids=group_ids, opts=opts)
