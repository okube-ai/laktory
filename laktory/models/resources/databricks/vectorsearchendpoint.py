from typing import Literal
from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource

# class VectorSearchEndpointLookup(ResourceLookup):
#     """
#     Parameters
#     ----------
#     id:
#         The ID of the Vector Search Endpoint warehouse.
#     """
#
#     id: str = Field(serialization_alias="id")


class VectorSearchEndpoint(BaseModel, PulumiResource, TerraformResource):
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

    endpoint_type: Literal["STANDARD"] = Field(
        "STANDARD",
        description="Type of Vector Search Endpoint. Currently only accepting single value: STANDARD",
    )  # required
    name: str = Field(None, description="Name of the Vector Search Endpoint to create.")

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

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_vector_search_endpoint"

    # @property
    # def terraform_resource_lookup_type(self) -> str:
    #     return "databricks_sql_warehouse"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
