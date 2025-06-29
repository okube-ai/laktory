from typing import Any
from typing import Literal
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class WarehouseCustomTag(BaseModel):
    key: str = Field(..., description="Tag key")
    value: str = Field(..., description="Tag value")


class WarehouseTags(BaseModel):
    custom_tags: list[WarehouseCustomTag] = Field([], description="Tags specifications")

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "custom_tags": "custom_tags",
        }


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


class Warehouse(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Warehouse

    Examples
    --------
    ```py
    from laktory import models

    warehouse = models.resources.databricks.Warehouse(
        name="default",
        cluster_size="2X-Small",
        auto_stop_mins=30,
        channel_name="CHANNEL_NAME_PREVIEW",
        enable_photon=True,
        enable_serverless_compute=True,
        access_controls=[
            {"group_name": "account users", "permission_level": "CAN_USE"}
        ],
    )
    ```
    """

    cluster_size: Literal[
        "2X-Small",
        "X-Small",
        "Small",
        "Medium",
        "Large",
        "X-Large",
        "2X-Large",
        "3X-Large",
        "4X-Large",
    ] = Field(..., description="The size of the clusters allocated to the endpoint")
    access_controls: list[AccessControl] = Field([], description="Access controls list")
    auto_stop_mins: int = Field(
        None,
        description="Time in minutes until an idle SQL warehouse terminates all clusters and stops.",
    )
    channel_name: Union[
        Literal["CHANNEL_NAME_CURRENT", "CHANNEL_NAME_PREVIEW"], str
    ] = Field(None, description="Channel specifications")
    # data_source_id
    enable_photon: bool = Field(None, description="If `True`, photon is enabled")
    enable_serverless_compute: bool = Field(
        None, description="If `True`, warehouse is serverless."
    )
    instance_profile_arn: str = Field(None, description="")
    jdbc_url: str = Field(None, description="JDBC connection string.")
    lookup_existing: WarehouseLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    max_num_clusters: int = Field(
        None,
        description="Maximum number of clusters available when a SQL warehouse is running.",
    )
    min_num_clusters: int = Field(
        None,
        description="Minimum number of clusters available when a SQL warehouse is running.",
    )
    name: str = Field(..., description="Warehouse name")
    num_clusters: int = Field(
        None, description="Fixed number of clusters when autoscaling is not enabled."
    )
    # odbc_params
    spot_instance_policy: Literal["COST_OPTIMIZED", "RELIABILITY_OPTIMIZED"] = Field(
        None, description="The spot policy to use for allocating instances to clusters."
    )
    # state
    tags: WarehouseTags = Field(
        None, description="Databricks tags all endpoint resources with these tags."
    )
    warehouse_type: Literal["CLASSIC", "PRO"] = Field(
        None, description="SQL warehouse type."
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
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
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:SqlEndpoint"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]

    @property
    def pulumi_properties(self):
        d = super().pulumi_properties
        if self._camel_serialization:
            d["channel"] = {"name": d.pop("channelName", None)}
        else:
            d["channel"] = {"name": d.pop("channel_name", None)}
        return d

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_sql_endpoint"

    @property
    def terraform_resource_lookup_type(self) -> str:
        return "databricks_sql_warehouse"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes

    @property
    def terraform_properties(self) -> dict:
        d = super().terraform_properties
        d["channel"] = {"name": d.pop("channel_name", None)}
        return d
