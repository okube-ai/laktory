from typing import Union

import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.schema import Schema

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiSchema(PulumiResourcesEngine):

    @property
    def provider(self):
        return "databricks"

    def __init__(
            self,
            name=None,
            schema: Schema = None,
            opts=None,
    ):

        if name is None:
            name = f"schema-{schema.full_name}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
        )

        # Schema
        self.schema = databricks.Schema(
            f"schema-{schema.full_name}",
            name=schema.name,
            catalog_name=schema.catalog_name,
            force_destroy=schema.force_destroy,
            opts=opts,
        )

        # Schema grants
        _opts = opts.merge(pulumi.ResourceOptions(depends_on=self.schema))
        if schema.grants:
            self.grants = databricks.Grants(
                f"grants-{schema.full_name}",
                schema=schema.full_name,
                grants=[
                    databricks.GrantsGrantArgs(principal=g.principal, privileges=g.privileges) for g in schema.grants
                ],
                opts=_opts,
            )

        # Schema volumes
        if schema.volumes:
            for v in schema.volumes:
                v._resources = v.deploy_with_pulumi(opts=pulumi.ResourceOptions(parent=self.schema))

        # TODO: Schema tables
