from typing import Union

from laktory.models.resources.databricks.vectorsearchendpoint_base import (
    VectorSearchEndpointBase,
)
from laktory.models.resources.pulumiresource import PulumiResource

# class VectorSearchEndpointLookup(ResourceLookup):
#     """
#     Parameters
#     ----------
#     id:
#         The ID of the Vector Search Endpoint warehouse.
#     """
#
#     id: str = Field(serialization_alias="id")


class VectorSearchEndpoint(VectorSearchEndpointBase, PulumiResource):
    """
    Databricks Warehouse

    Examples
    --------
    ```py
    from laktory import models

    endpoint = models.resources.databricks.VectorSearchEndpoint(
        endpoint_type="STANDARD",
        name="default",
    )
    ```
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:VectorSearchEndpoint"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    # @property
    # def terraform_resource_lookup_type(self) -> str:
    #     return "databricks_sql_warehouse"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
