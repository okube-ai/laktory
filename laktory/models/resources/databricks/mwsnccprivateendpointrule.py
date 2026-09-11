from laktory.models.resources.databricks.mwsnccprivateendpointrule_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwsnccprivateendpointrule_base import (
    MwsNccPrivateEndpointRuleBase,
)


class MwsNccPrivateEndpointRule(MwsNccPrivateEndpointRuleBase):
    """
    Databricks Mws Ncc Private Endpoint Rule

    Examples
    --------
    ```py
    import io

    from laktory import models

    rule_yaml = '''
    network_connectivity_config_id: ${resources.ncc-prod.network_connectivity_config_id}
    resource_names:
    - my-bucket
    endpoint_service: com.amazonaws.vpce.us-east-1.vpce-svc-00000000000000000
    '''
    rule = models.resources.databricks.MwsNccPrivateEndpointRule.model_validate_yaml(
        io.StringIO(rule_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS NCC Private Endpoint Rule](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_ncc_private_endpoint_rule)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.network_connectivity_config_id}-{self.resource_id or self.endpoint_service or ''}"
