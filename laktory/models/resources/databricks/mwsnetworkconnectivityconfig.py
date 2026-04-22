from typing import Union

from pydantic import Field

from laktory.models.resources.databricks.mwsnccbinding import MwsNccBinding
from laktory.models.resources.databricks.mwsnetworkconnectivityconfig_base import (
    MwsNetworkConnectivityConfigBase,
)
from laktory.models.resources.pulumiresource import PulumiResource


class MwsNetworkConnectivityConfig(MwsNetworkConnectivityConfigBase, PulumiResource):
    """
    Databricks Mws Network Connectivity Config

    Examples
    --------
    ```py
    ```
    """

    workspace_bindings: list[MwsNccBinding] = Field(None, description="")

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - workspace bindings
        """
        resources = []

        if self.workspace_bindings:
            for b in self.workspace_bindings:
                b.network_connectivity_config_id = f"${{resources.{self.resource_name}.network_connectivity_config_id}}"
                resources += [b]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MwsNetworkConnectivityConfig"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "workspace_bindings",
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
