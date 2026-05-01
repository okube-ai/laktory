# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_entitlements
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class EntitlementsBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_entitlements`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    allow_cluster_create: bool | None = Field(
        None,
        description="Allow the principal to have [cluster](cluster.md) create privileges. Defaults to false. More fine grained permissions could be assigned with [databricks_permissions](permissions.md#Cluster-usage) and `cluster_id` argument. Everyone without `allow_cluster_create` argument set, but with [permission to use](permissions.md#Cluster-Policy-usage) Cluster Policy would be able to create clusters, but within boundaries of that specific policy",
    )
    allow_instance_pool_create: bool | None = Field(
        None,
        description="Allow the principal to have [instance pool](instance_pool.md) create privileges. Defaults to false. More fine grained permissions could be assigned with [databricks_permissions](permissions.md#Instance-Pool-usage) and [instance_pool_id](permissions.md#instance_pool_id) argument",
    )
    databricks_sql_access: bool | None = Field(
        None,
        description="This is a field to allow the principal to have access to [Databricks SQL](https://databricks.com/product/databricks-sql)  UI, [Databricks One](https://docs.databricks.com/aws/en/workspace/databricks-one#who-can-access-databricks-one) and through [databricks_sql_endpoint](sql_endpoint.md)",
    )
    group_id: str | None = Field(
        None, description="Canonical unique identifier for the group"
    )
    service_principal_id: str | None = Field(
        None, description="Canonical unique identifier for the service principal"
    )
    user_id: str | None = Field(
        None, description="Canonical unique identifier for the user"
    )
    workspace_access: bool | None = Field(
        None,
        description="This is a field to allow the principal to have access to a Databricks Workspace UI and [Databricks One](https://docs.databricks.com/aws/en/workspace/databricks-one#who-can-access-databricks-one)",
    )
    workspace_consume: bool | None = Field(
        None,
        description="This is a field to allow the principal to have access only to [Databricks One](https://docs.databricks.com/aws/en/workspace/databricks-one#who-can-access-databricks-one).  Couldn't be used with `workspace_access` or `databricks_sql_access`",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_entitlements"


__all__ = ["EntitlementsBase"]
