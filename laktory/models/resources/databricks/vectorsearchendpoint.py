from typing import Union

from laktory.models.resources.databricks.vectorsearchendpoint_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.vectorsearchendpoint_base import (
    VectorSearchEndpointBase,
)

# class VectorSearchEndpointLookup(ResourceLookup):
#     """
#     Parameters
#     ----------
#     id:
#         The ID of the Vector Search Endpoint warehouse.
#     """
#
#     id: str = Field(serialization_alias="id")


class VectorSearchEndpoint(VectorSearchEndpointBase):
    """
    Databricks Vector Search Endpoint

    Examples
    --------
    ```py
    import io

    from laktory import models

    endpoint_yaml = '''
    name: default
    endpoint_type: STANDARD
    '''
    endpoint = models.resources.databricks.VectorSearchEndpoint.model_validate_yaml(
        io.StringIO(endpoint_yaml)
    )
    ```

    References
    ----------

    * [Databricks Vector Search](https://docs.databricks.com/en/generative-ai/vector-search.html)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    # @property
    # def terraform_resource_lookup_type(self) -> str:
    #     return "databricks_sql_warehouse"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]
