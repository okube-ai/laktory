from abc import abstractmethod
from typing import Union
from laktory.models.resources.bresource import BaseResource

variables = {}


class PulumiResource(BaseResource):

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    @abstractmethod
    def pulumi_resource_type(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
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

    def deploy_with_pulumi(self, opts=None):
        for r in self.all_resources:
            properties = r.pulumi_properties
            properties = self.resolve_vars(properties, target="pulumi_py")
            _r = r.pulumi_cls(r.resource_name, **properties, opts=opts)

            # TODO: Store other properties (like url, etc.).
            variables[f"{r.resource_name}.id"] = _r.id
