from typing import Any
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.databricks.sqlendpoint_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.sqlendpoint_base import SqlEndpointBase


class WarehouseLookup(ResourceLookup):
    id: str = Field(
        serialization_alias="id",
        default=None,
        description="The ID of the SQL warehouse.",
    )
    name: str = Field(
        None,
        description="""
        Name of the SQL warehouse. Name of the SQL warehouse to search (case-sensitive). Argument only supported by 
        Terraform IaC backend.
        """,
    )

    @model_validator(mode="after")
    def at_least_one(self) -> Any:
        if self.id is None and self.name is None:
            raise ValueError("At least `id` or `name` must be set.")

        if not (self.id is None or self.name is None):
            raise ValueError("Only one of `id` or `name` must be set.")

        return self


class Warehouse(SqlEndpointBase):
    """
    Databricks Warehouse

    Examples
    --------
    ```py
    import io

    from laktory import models

    warehouse_yaml = '''
    name: default
    cluster_size: 2X-Small
    auto_stop_mins: 30
    channel_name: CHANNEL_NAME_PREVIEW
    enable_photon: true
    enable_serverless_compute: true
    access_controls:
    - group_name: account users
      permission_level: CAN_USE
    '''
    warehouse = models.resources.databricks.Warehouse.model_validate_yaml(
        io.StringIO(warehouse_yaml)
    )
    ```

    References
    ----------

    * [Databricks SQL Warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    lookup_existing: WarehouseLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
        """
        - warehouse permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    sql_endpoint_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_lookup_type(self) -> str:
        return "databricks_sql_warehouse"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]

    @property
    def terraform_properties(self) -> dict:
        d = super().terraform_properties
        return d
