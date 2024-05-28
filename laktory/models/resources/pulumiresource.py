from abc import abstractmethod
from typing import Union
from typing import Any

from laktory._settings import settings
from laktory._parsers import _snake_to_camel
from laktory.models.resources.baseresource import BaseResource


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
