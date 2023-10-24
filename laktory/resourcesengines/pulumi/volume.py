from typing import Union

import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.sql.volume import Volume

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiVolume(PulumiResourcesEngine):
    @property
    def provider(self):
        return "databricks"

    def __init__(
        self,
        name=None,
        volume: Volume = None,
        opts=None,
    ):
        if name is None:
            name = f"volume-{volume.full_name}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
        )

        # Volume
        self.volume = databricks.Volume(
            f"volume-{volume.full_name}",
            opts=opts,
            **volume.model_pulumi_dump(),
        )

        # Volume grants
        _opts = opts.merge(pulumi.ResourceOptions(depends_on=self.volume))
        if volume.grants:
            self.grants = databricks.Grants(
                f"grants-{volume.full_name}",
                volume=volume.full_name,
                grants=[
                    databricks.GrantsGrantArgs(
                        principal=g.principal, privileges=g.privileges
                    )
                    for g in volume.grants
                ],
                opts=_opts,
            )
