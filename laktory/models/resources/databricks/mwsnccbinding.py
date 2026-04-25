from laktory.models.resources.databricks.mwsnccbinding_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwsnccbinding_base import MwsNccBindingBase


class MwsNccBinding(MwsNccBindingBase):
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
