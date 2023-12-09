import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.sql.table import Table

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiTable(PulumiResourcesEngine):
    @property
    def provider(self):
        return "databricks"

    def __init__(
        self,
        name=None,
        table: Table = None,
        opts=None,
    ):
        if name is None:
            name = table.resource_name
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
        )

        # Table
        if table.builder.pipeline_name is None:
            self.table = databricks.SqlTable(
                name,
                opts=opts,
                **table.model_pulumi_dump(),
            )

        # Schema grants
        _opts = opts.merge(pulumi.ResourceOptions(depends_on=self.table))
        if table.grants:
            self.grants = databricks.Grants(
                f"grants-{name}",
                table=table.full_name,
                grants=[
                    databricks.GrantsGrantArgs(
                        principal=g.principal, privileges=g.privileges
                    )
                    for g in table.grants
                ],
                opts=_opts,
            )
