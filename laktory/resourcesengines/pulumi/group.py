import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.databricks.group import Group


class PulumiGroup(PulumiResourcesEngine):
    @property
    def provider(self):
        return "databricks"

    def __init__(
        self,
        name=None,
        group: Group = None,
        opts=None,
    ):
        if name is None:
            name = group.resource_name
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        if group.id is None:
            self.group = databricks.Group(name, opts=opts, **group.model_pulumi_dump())
            group.id = self.group.id
        else:
            self.group = databricks.Group.get(name, id=group.id)
