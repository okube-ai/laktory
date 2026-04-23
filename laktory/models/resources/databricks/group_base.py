# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_group
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class GroupBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_group`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    display_name: str = Field(
        ..., description="This is the display name for the given group"
    )
    acl_principal_id: str | None = Field(
        None,
        description="identifier for use in [databricks_access_control_rule_set](access_control_rule_set.md), e.g. `groups/Some Group`",
    )
    allow_cluster_create: bool | None = Field(
        None,
        description="This is a field to allow the group to have [cluster](cluster.md) create privileges. More fine grained permissions could be assigned with [databricks_permissions](permissions.md#Cluster-usage) and [cluster_id](permissions.md#cluster_id) argument. Everyone without `allow_cluster_create` argument set, but with [permission to use](permissions.md#Cluster-Policy-usage) Cluster Policy would be able to create clusters, but within boundaries of that specific policy",
    )
    allow_instance_pool_create: bool | None = Field(
        None,
        description="This is a field to allow the group to have [instance pool](instance_pool.md) create privileges. More fine grained permissions could be assigned with [databricks_permissions](permissions.md#Instance-Pool-usage) and [instance_pool_id](permissions.md#instance_pool_id) argument",
    )
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host",
    )
    databricks_sql_access: bool | None = Field(
        None,
        description="This is a field to allow the group to have access to [Databricks SQL](https://databricks.com/product/databricks-sql)  UI, [Databricks One](https://docs.databricks.com/aws/en/workspace/databricks-one#who-can-access-databricks-one) and through [databricks_sql_endpoint](sql_endpoint.md)",
    )
    external_id: str | None = Field(
        None, description="ID of the group in an external identity provider"
    )
    force: bool | None = Field(
        None,
        description="Ignore `cannot create group: Group with name X already exists.` errors and implicitly import the specific group into Terraform state, enforcing entitlements defined in the instance of resource. _This functionality is experimental_ and is designed to simplify corner cases, like Azure Active Directory synchronisation",
    )
    url: str | None = Field(None)
    workspace_access: bool | None = Field(
        None,
        description="This is a field to allow the group to have access to a Databricks Workspace UI and [Databricks One](https://docs.databricks.com/aws/en/workspace/databricks-one#who-can-access-databricks-one)",
    )
    workspace_consume: bool | None = Field(
        None,
        description="This is a field to allow the group to have access only to [Databricks One](https://docs.databricks.com/aws/en/workspace/databricks-one#who-can-access-databricks-one).  Couldn't be used with `workspace_access` or `databricks_sql_access`",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_group"


__all__ = ["GroupBase"]
