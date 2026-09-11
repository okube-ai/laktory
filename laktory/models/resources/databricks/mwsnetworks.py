from laktory.models.resources.databricks.mwsnetworks_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwsnetworks_base import MwsNetworksBase


class MwsNetworks(MwsNetworksBase):
    """
    Databricks Mws Networks

    Examples
    --------
    ```py
    import io

    from laktory import models

    network_yaml = '''
    account_id: ${vars.databricks_account_id}
    network_name: my-network
    vpc_id: vpc-00000000
    subnet_ids:
    - subnet-00000000
    - subnet-00000001
    security_group_ids:
    - sg-00000000
    '''
    network = models.resources.databricks.MwsNetworks.model_validate_yaml(
        io.StringIO(network_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS Networks](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_networks)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.network_name
