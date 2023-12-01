import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.directory import Directory


class PulumiDirectory(PulumiResourcesEngine):
    @property
    def provider(self):
        return "databricks"

    def __init__(
        self,
        name=None,
        directory: Directory = None,
        opts=None,
    ):
        if name is None:
            name = f"directory-{directory.key}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        self.directory = databricks.Directory(
            f"directory-{directory.key}", opts=opts, **directory.model_pulumi_dump()
        )
