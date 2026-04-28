from typing import Union

from pydantic import Field

from laktory.models.resources.databricks.mwsnccbinding import MwsNccBinding
from laktory.models.resources.databricks.mwsnetworkconnectivityconfig_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwsnetworkconnectivityconfig_base import (
    MwsNetworkConnectivityConfigBase,
)


class MwsNetworkConnectivityConfig(MwsNetworkConnectivityConfigBase):
    """
    Databricks Mws Network Connectivity Config

    Examples
    --------
    ```py
    import io

    from laktory import models

    ncc_yaml = '''
    name: ncc-prod
    region: eastus
    '''
    ncc = models.resources.databricks.MwsNetworkConnectivityConfig.model_validate_yaml(
        io.StringIO(ncc_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS Network Connectivity Config](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_network_connectivity_config)
    """

    workspace_bindings: list[MwsNccBinding] = Field(None, description="")

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "workspace_bindings",
        ]
