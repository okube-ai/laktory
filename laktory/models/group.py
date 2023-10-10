from laktory.models.base import BaseModel
from laktory.models.resources import Resources


class Group(BaseModel, Resources):
    display_name: str
    allow_cluster_create: bool = False
    workspace_access: bool = True
    user_names: list[str] = None

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    def deploy_with_pulumi(self, name=None, opts=None):
        from laktory.resourcesengines.pulumi.group import PulumiGroup
        return PulumiGroup(name=name, group=self, opts=opts)
