import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.group import Group


class PulumiGroup(PulumiResourcesEngine):

    @property
    def provider(self):
        return "databricks"

    def __init__(
            self,
            name=None,
            group: Group = None,
            opts=None,
            **kwargs
    ):
        if name is None:
            name = f"group-{group.display_name}"
        super().__init__(self.t, name, {}, opts)

        kwargs["opts"] = kwargs.get("opts", pulumi.ResourceOptions())
        kwargs["opts"].parent = self
        kwargs["opts"].delete_before_replace = getattr(kwargs["opts"], "delete_before_replace", True)

        self.group = databricks.Group(
            f"group-{group.display_name}",
            display_name=group.display_name,
            allow_cluster_create=group.allow_cluster_create,
            **kwargs,
        )