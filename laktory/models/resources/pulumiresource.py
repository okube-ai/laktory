from typing import ClassVar
from typing import Any
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
    def pulumi_excludes(self) -> list[str]:
        """List of fields to exclude when dumping model to pulumi"""
        return []

    @property
    def pulumi_renames(self) -> dict[str, str]:
        """Map of fields to rename when dumping model to pulumi"""
        return {}

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def pulumi_yaml_dump(self, *args, to_stack=False, **kwargs) -> dict[str, Any]:
        return {
            "type": self.pulumi_resource_type,
            "properties": self.pulumi_properties()
        }

    def pulumi_properties(self, *args, **kwargs) -> dict:
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
        kwargs["exclude"] = self.pulumi_excludes
        d = super().model_dump(*args, **kwargs)
        for k, v in self.pulumi_renames.items():
            d[v] = d.pop(k)
        d = self.inject_vars(d)
        return d
