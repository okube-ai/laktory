from laktory.models.resources.databricks.mwsvpcendpoint_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mwsvpcendpoint_base import MwsVpcEndpointBase


class MwsVpcEndpoint(MwsVpcEndpointBase):
    """
    Databricks Mws Vpc Endpoint

    Examples
    --------
    ```py
    import io

    from laktory import models

    endpoint_yaml = '''
    vpc_endpoint_name: my-vpc-endpoint
    aws_vpc_endpoint_id: vpce-00000000
    region: us-east-1
    '''
    endpoint = models.resources.databricks.MwsVpcEndpoint.model_validate_yaml(
        io.StringIO(endpoint_yaml)
    )
    ```

    References
    ----------

    * [Databricks MWS VPC Endpoint](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_vpc_endpoint)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.vpc_endpoint_name
