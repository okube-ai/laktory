from laktory.models.resources.databricks.mwsnccbinding_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwsnccbinding_base import MwsNccBindingBase
from laktory.models.resources.pulumiresource import PulumiResource


class MwsNccBinding(MwsNccBindingBase, PulumiResource):
    """
    Databricks Mws Network Connectivity Config Binding

    Examples
    --------
    ```py
    ```
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self):
        return f"{self.network_connectivity_config_id}-{self.workspace_id}"

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MwsNccBinding"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
