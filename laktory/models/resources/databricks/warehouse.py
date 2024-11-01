from typing import Literal
from typing import Union
from pydantic import Field
from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions


class WarehouseCustomTag(BaseModel):
    """
    Warehouse Custom Tag specifications

    Attributes
    ----------
    key:
        Tag key
    value:
        Tag value
    """

    key: str
    value: str


class WarehouseTags(BaseModel):
    """
    Warehouse Tags specifications

    Attributes
    ----------
    custom_tags:
        Tags specifications
    """

    custom_tags: list[WarehouseCustomTag] = []

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "custom_tags": "custom_tags",
        }


class WarehouseLookup(ResourceLookup):
    """
    Attributes
    ----------
    id:
        The ID of the SQL warehouse.
    """

    id: str = Field(serialization_alias="id")


class Warehouse(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Warehouse

    Attributes
    ----------
    access_controls:
        Warehouse access controls
    auto_stop_mins:
        Time in minutes until an idle SQL warehouse terminates all clusters and stops.
    channel_name:
        Channel specifications
    cluster_size:
        The size of the clusters allocated to the endpoint
    enable_photon:
        If `True`, photon is enabled
    enable_serverless_compute:
        If `True`, warehouse is serverless.
        See details [here](https://www.pulumi.com/registry/packages/databricks/api-docs/sqlendpoint/)
    instance_profile_arn:
        TODO
    jdbc_url:
        JDBC connection string.
    lookup_existing:
        Specifications for looking up existing resource. Other attributes will
        be ignored.
    max_num_clusters:
        Maximum number of clusters available when a SQL warehouse is running.
    min_num_clusters:
        Minimum number of clusters available when a SQL warehouse is running.
    name:
        Warehouse name
    num_clusters:
        Fixed number of clusters when autoscaling is not enabled.
    spot_instance_policy:
        The spot policy to use for allocating instances to clusters.
    tags:
        Databricks tags all endpoint resources with these tags.
    warehouse_type:
        SQL warehouse type.

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
        access_controls=[{"group_name": "account users", "permission_level": "CAN_USE"}],
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
    ]
    access_controls: list[AccessControl] = []
    auto_stop_mins: int = None
    channel_name: Union[
        Literal["CHANNEL_NAME_CURRENT", "CHANNEL_NAME_PREVIEW"], str
    ] = None
    # data_source_id
    enable_photon: bool = None
    enable_serverless_compute: bool = None
    instance_profile_arn: str = None
    jdbc_url: str = None
    lookup_existing: WarehouseLookup = Field(None, exclude=True)
    max_num_clusters: int = None
    min_num_clusters: int = None
    name: str
    num_clusters: int = None
    # odbc_params
    spot_instance_policy: Union[
        Literal["COST_OPTIMIZED", "RELIABILITY_OPTIMIZED"], str
    ] = None
    # state
    tags: WarehouseTags = None
    warehouse_type: Union[Literal["CLASSIC", "PRO"], str] = None

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
        if settings.camel_serialization:
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
