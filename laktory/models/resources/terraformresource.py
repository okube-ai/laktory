from abc import abstractmethod
from typing import Union
from typing import Any
from laktory.models.resources.baseresource import BaseResource
#
# pulumi_outputs = {}
# """Pulumi outputs for all deployed resources. Updated during deployment."""
#
# pulumi_resources = {}
# """All pulumi deployed resources objects. Updated during deployment."""
#


class TerraformResource(BaseResource):
    """
    Parent class for all Laktory models deployable with Terraform IaC backend.
    """
    # _pulumi_resources: dict[str, Any] = {}

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    @abstractmethod
    def terraform_resource_type(self) -> str:
        raise NotImplementedError()

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        """List of fields to exclude when dumping model to pulumi"""
        return []

    @property
    def terraform_renames(self) -> dict[str, str]:
        """Map of fields to rename when dumping model to pulumi"""
        return {}
