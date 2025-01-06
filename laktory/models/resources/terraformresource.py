from abc import abstractmethod
from typing import Union

from laktory.models.resources.baseresource import BaseResource


class TerraformResource(BaseResource):
    """
    Parent class for all Laktory models deployable with Terraform IaC backend.
    """

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    @abstractmethod
    def terraform_resource_type(self) -> str:
        raise NotImplementedError()

    @property
    def terraform_resource_lookup_type(self) -> str:
        return self.terraform_resource_type

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        """List of fields to exclude when dumping model to terraform"""
        return []

    @property
    def terraform_renames(self) -> dict[str, str]:
        """Map of fields to rename when dumping model to terraform"""
        return {}

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_properties(self) -> dict:
        """
        Resources properties formatted for terraform:

        * Serialization (model dump)
        * Removal of excludes defined in `self.terraform_excludes`
        * Renaming of keys according to `self.terraform_renames`
        * Injection of variables

        Returns
        -------
        :
            Terraform-safe model dump
        """
        # `exclude_unset` should be used instead of exclude_none, but for now
        # we set default values to facilitate instantiation of models within
        # other models (permissions in MwsPermissionAssignment for example).
        # A better approach should be used.
        d = super().model_dump(exclude=self.terraform_excludes, exclude_none=True)
        for k, v in self.terraform_renames.items():
            if k in d:
                d[v] = d.pop(k)

        # Add options
        for k in ["depends_on", "provider"]:
            value = self.options.model_dump(exclude_unset=True).get(k, None)
            if value:
                d[k] = value

        d["lifecycle"] = {}
        for k in ["ignore_changes"]:
            value = self.options.model_dump(exclude_unset=True).get(k, None)
            if value:
                d["lifecycle"][k] = value
        if d["lifecycle"] == {}:
            del d["lifecycle"]

        return d
