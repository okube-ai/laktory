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
            **kwargs
    ):

        if name is None:
            name = f"catalog-{schema.full_name}"
        super().__init__(self.t, name, {}, opts)

        kwargs["opts"] = kwargs.get("opts", pulumi.ResourceOptions())
        kwargs["opts"].parent = self

        # Schema
        self.schema = databricks.Schema(
            f"schema-{schema.full_name}",
            name=schema.name,
            catalog_name=schema.catalog_name,
            force_destroy=schema.force_destroy,
            **kwargs
        )

        # Schema grants
        if schema.grants:
            _grants = databricks.Grants(
                f"grants-{schema.full_name}",
                schema=self.schema.name,
                grants=[
                    databricks.GrantsGrantArgs(principal=g.principal, privileges=g.privileges) for g in schema.grants
                ],
                **kwargs
            )

        # Schema volumes
        if schema.volumes:
            for v in schema.volumes:
                v.deploy_with_pulumi(opts=pulumi.ResourceOptions(parent=self))

        # TODO: Schema tables
