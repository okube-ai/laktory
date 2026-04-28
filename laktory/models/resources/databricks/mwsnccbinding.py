from laktory.models.resources.databricks.mwsnccbinding_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwsnccbinding_base import MwsNccBindingBase


class MwsNccBinding(MwsNccBindingBase):
    """
    Databricks Mws Network Connectivity Config Binding

    Examples
    --------
    ```py
    import io

    from laktory import models

    binding_yaml = '''
    network_connectivity_config_id: ${resources.ncc-prod.network_connectivity_config_id}
    workspace_id: 1234567890
    '''
    binding = models.resources.databricks.MwsNccBinding.model_validate_yaml(
        io.StringIO(binding_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS NCC Binding](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_ncc_binding)
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
