from abc import abstractmethod
from typing import Union
from typing import Any
from laktory.models.resources.baseresource import BaseResource

pulumi_outputs = {}
"""Store pulumi outputs for deployed resources"""

pulumi_resources = {}
"""Store pulumi deployed resources"""


class PulumiResource(BaseResource):
    _pulumi_resources: dict[str, Any] = {}

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
        d = super().model_dump(exclude=self.pulumi_excludes, exclude_none=True)
        for k, v in self.pulumi_renames.items():
            d[v] = d.pop(k)
        d = self.inject_vars(d)
        return d

    def to_pulumi(self, opts=None):
        from pulumi import ResourceOptions

        self._pulumi_resources = {}

        for r in self.resources:
            # Properties
            properties = r.pulumi_properties
            properties = self.inject_vars(properties, target="pulumi_py")

            # Options
            _opts = self.options.model_dump()
            if _opts is None:
                _opts = {}
            _opts = self.inject_vars(_opts, target="pulumi_py")
            _opts = ResourceOptions(**_opts)
            if opts is not None:
                _opts = ResourceOptions.merge(_opts, opts)

            _r = r.pulumi_cls(r.resource_name, **properties, opts=_opts)

            # Save resource
            self._pulumi_resources[r.resource_name] = _r
            pulumi_resources[r.resource_name] = _r

            # Save resource outputs
            # TODO: Store other properties (like url, etc.).
            for k in [
                "id",
                "object_id",
            ]:
                if hasattr(_r, k):
                    pulumi_outputs[f"{r.resource_name}.{k}"] = getattr(_r, k)

        # Return resources
        return self._pulumi_resources
