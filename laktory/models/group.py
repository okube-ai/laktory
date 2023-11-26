from typing import Union
from laktory.models.base import BaseModel
from laktory.models.resources import Resources


class Group(BaseModel, Resources):
    display_name: str
    allow_cluster_create: bool = False
    workspace_access: bool = None
    user_names: list[str] = None
    id: Union[str, None] = None

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["user_names", "id"]

    def deploy_with_pulumi(self, name=None, opts=None):
        from laktory.resourcesengines.pulumi.group import PulumiGroup

        return PulumiGroup(name=name, group=self, opts=opts)
