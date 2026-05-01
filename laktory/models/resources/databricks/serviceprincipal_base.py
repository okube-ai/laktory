# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_service_principal
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel, PluralField
from laktory.models.resources.terraformresource import TerraformResource


class ServicePrincipalBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_service_principal`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    acl_principal_id: str | None = Field(
        None,
        description="identifier for use in [databricks_access_control_rule_set](access_control_rule_set.md), e.g. `servicePrincipals/00000000-0000-0000-0000-000000000000`",
    )
    active: bool | None = Field(
        None,
        description="Either service principal is active or not. True by default, but can be set to false in case of service principal deactivation with preserving service principal assets",
    )
    allow_cluster_create: bool | None = Field(
        None,
        description="Allow the service principal to have [cluster](cluster.md) create privileges. Defaults to false. More fine grained permissions could be assigned with [databricks_permissions](permissions.md#Cluster-usage) and `cluster_id` argument. Everyone without `allow_cluster_create` argument set, but with [permission to use](permissions.md#Cluster-Policy-usage) Cluster Policy would be able to create clusters, but within the boundaries of that specific policy",
    )
    allow_instance_pool_create: bool | None = Field(
        None,
        description="Allow the service principal to have [instance pool](instance_pool.md) create privileges. Defaults to false. More fine grained permissions could be assigned with [databricks_permissions](permissions.md#Instance-Pool-usage) and [instance_pool_id](permissions.md#instance_pool_id) argument",
    )
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host",
    )
    application_id: str | None = Field(None)
    databricks_sql_access: bool | None = Field(
        None,
        description="This is a field to allow the service principal to have access to [Databricks SQL](https://databricks.com/product/databricks-sql) feature through [databricks_sql_endpoint](sql_endpoint.md)",
    )
    disable_as_user_deletion: bool | None = Field(
        None,
        description="Deactivate the service principal when deleting the resource, rather than deleting the service principal entirely. Defaults to `true` when the provider is configured at the account-level and `false` when configured at the workspace-level. This flag is exclusive to force_delete_repos and force_delete_home_dir flags",
    )
    display_name: str | None = Field(
        None,
        description="This is an alias for the service principal and can be the full name of the service principal",
    )
    external_id: str | None = Field(
        None, description="ID of the service principal in an external identity provider"
    )
    force: bool | None = Field(
        None,
        description="Ignore `cannot create service principal: Service principal with application ID X already exists` errors and implicitly import the specified service principal into Terraform state, enforcing entitlements defined in the instance of resource. _This functionality is experimental_ and is designed to simplify corner cases, like Azure Active Directory synchronisation",
    )
    force_delete_home_dir: bool | None = Field(
        None,
        description="This flag determines whether the service principal's home directory is deleted when the user is deleted. It will have no impact when in the accounts SCIM API. False by default",
    )
    force_delete_repos: bool | None = Field(
        None,
        description="This flag determines whether the service principal's repo directory is deleted when the user is deleted. It will have no impact when in the accounts SCIM API. False by default",
    )
    home: str | None = Field(
        None,
        description="Home folder of the service principal, e.g. `/Users/00000000-0000-0000-0000-000000000000`",
    )
    repos: str | None = Field(
        None,
        description="Personal Repos location of the service principal, e.g. `/Repos/00000000-0000-0000-0000-000000000000`",
    )
    workspace_access: bool | None = Field(
        None,
        description="This is a field to allow the service principal to have access to a Databricks Workspace",
    )
    workspace_consume: bool | None = Field(
        None,
        description="This is a field to allow the service principal to have access to a Databricks Workspace as consumer, with limited access to workspace UI.  Couldn't be used with `workspace_access` or `databricks_sql_access`",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_service_principal"


__all__ = ["ServicePrincipalBase"]
