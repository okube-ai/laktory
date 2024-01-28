from abc import abstractmethod
from typing import Union
from typing import Any
from laktory._settings import settings
from laktory._parsers import _snake_to_camel
from laktory.models.resources.baseresource import BaseResource

pulumi_outputs = {}
"""Pulumi outputs for all deployed resources. Updated during deployment."""

pulumi_resources = {}
"""All pulumi deployed resources objects. Updated during deployment."""


class PulumiResource(BaseResource):
    """
    Parent class for all Laktory models deployable with Pulumi IaC backend.
    """

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
        Resources properties formatted for pulumi:

        * Serialization (model dump)
        * Removal of excludes defined in `self.pulumi_excludes`
        * Renaming of keys according to `self.pulumi_renames`
        * Injection of variables

        Returns
        -------
        :
            Pulumi-safe model dump
        """
        d = super().model_dump(exclude=self.pulumi_excludes, exclude_none=True)
        for k, v in self.pulumi_renames.items():
            if settings.camel_serialization:
                k = _snake_to_camel(k)
                v = _snake_to_camel(v)
            if k in d:
                d[v] = d.pop(k)
        d = self.inject_vars(d)
        return d

    def to_pulumi(self, opts=None):
        """
        Instantiate one or multiple pulumi objects related to the model. This
        function would trigger a deployment in the context of a `pulumi up`

        Parameters
        ----------
        opts:
            Resource options for deployment

        Returns
        -------
        :
            Deployed pulumi resources
        """
        from pulumi import ResourceOptions

        self._pulumi_resources = {}

        for r in self.core_resources:
            # Properties
            properties = r.pulumi_properties
            properties = self.inject_vars(properties)

            # Options
            _opts = r.options.model_dump()
            if _opts is None:
                _opts = {}
            _opts = self.inject_vars(_opts)
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
                "path",
            ]:
                if hasattr(_r, k):
                    pulumi_outputs[f"{r.resource_name}.{k}"] = getattr(_r, k)

        # Return resources
        return self._pulumi_resources
