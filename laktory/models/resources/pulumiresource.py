import os
from typing import Any
from typing import Union
from laktory.models.resources.bresource import BaseResource


class PulumiResource(BaseResource):

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self):
        raise NotImplementedError()

    @property
    def pulumi_cls(self):
        raise NotImplementedError()

    @property
    def base_pulumi_excludes(self) -> list[str]:
        """List of fields from base class to exclude when dumping model to pulumi"""
        return ["resource_name"]

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        """List of fields to exclude when dumping model to pulumi"""
        return []

    @property
    def pulumi_renames(self) -> dict[str, str]:
        """Map of fields to rename when dumping model to pulumi"""
        return {}

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_properties(self) -> dict:
        """
        Dump model and customize it to be used as an input for a pulumi
        resource:

        * dump the model
        * remove excludes defined in `self.pulumi_excludes`
        * rename keys according to `self.pulumi_renames`
        * inject variables

        Returns
        -------
        :
            Pulumi-safe model dump
        """
        excludes = self.base_pulumi_excludes
        if isinstance(self.pulumi_excludes, dict):
            excludes = {k: True for k in excludes}
            excludes.update(self.pulumi_excludes)
        else:
            excludes = self.base_pulumi_excludes + self.pulumi_excludes

        d = super().model_dump(exclude=excludes, exclude_none=True)
        for k, v in self.pulumi_renames.items():
            d[v] = d.pop(k)
        d = self.inject_vars(d)
        return d
