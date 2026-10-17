from pydantic import Field

from laktory.models.resources.databricks.accessrequestdestinations_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.accessrequestdestinations_base import (
    AccessRequestDestinationsBase,
)
from laktory.models.resources.databricks.accessrequestdestinations_base import (
    AccessRequestDestinationsSecurable,
)


class AccessRequestDestinations(AccessRequestDestinationsBase):
    """
    Databricks Unity Catalog access request destinations

    Where notifications are sent (email, Slack, Microsoft Teams, webhook or
    the owner of the securable) when users request access to a catalog,
    schema, table or volume. Can also be declared directly on `Catalog`,
    `Schema`, `Table` and `Volume` with their `access_request_destinations`
    field. Maps to the Terraform `databricks_rfa_access_request_destinations`
    resource.

    Examples
    --------
    ```py
    import io

    from laktory import models

    destinations_yaml = '''
    securable:
      type: SCHEMA
      full_name: dev.finance
    destinations:
    - destination_type: EMAIL
      destination_id: finance-data-owners@okube.ai
    '''
    destinations = (
        models.resources.databricks.AccessRequestDestinations.model_validate_yaml(
            io.StringIO(destinations_yaml)
        )
    )
    ```

    References
    ----------

    * [Databricks Access Requests](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-request-destinations)
    * [Terraform databricks_rfa_access_request_destinations](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/rfa_access_request_destinations)
    """

    # Required (base marks it optional)
    securable: AccessRequestDestinationsSecurable = Field(
        ...,
        description="The securable (`type` and `full_name`) for which the access request destinations are set",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.securable.type}-{self.securable.full_name}"
