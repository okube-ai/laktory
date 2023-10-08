import pulumi_databricks as databricks
from laktory.resources.basecomponentresource import BaseComponentResource


class DatabricksGroup(BaseComponentResource):

    @property
    def provider(self):
        return "databricks"

    @property
    def default_name(self) -> str:
        return f"group-{self.model.display_name}"

    def set_resources(self, **kwargs):
        self.group = databricks.Group(
            f"group-{self.model.display_name}",
            display_name=self.model.display_name,
            allow_cluster_create=self.model.allow_cluster_create,
            **kwargs,
        )
