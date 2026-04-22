# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_user
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class UserBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_user`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    user_name: str = Field(
        ...,
        description="This is the username of the given user and will be their form of access and identity.  Provided username will be converted to lower case if it contains upper case characters",
    )
    acl_principal_id: str | None = Field(
        None,
        description="identifier for use in [databricks_access_control_rule_set](access_control_rule_set.md), e.g. `users/mr.foo@example.com`",
    )
    active: bool | None = Field(
        None,
        description="Either user is active or not. True by default, but can be set to false in case of user deactivation with preserving user assets",
    )
    allow_cluster_create: bool | None = Field(
        None,
        description="Allow the user to have [cluster](cluster.md) create privileges. Defaults to false. More fine grained permissions could be assigned with [databricks_permissions](permissions.md#Cluster-usage) and `cluster_id` argument. Everyone without `allow_cluster_create` argument set, but with [permission to use](permissions.md#Cluster-Policy-usage) Cluster Policy would be able to create clusters, but within boundaries of that specific policy",
    )
    allow_instance_pool_create: bool | None = Field(
        None,
        description="Allow the user to have [instance pool](instance_pool.md) create privileges. Defaults to false. More fine grained permissions could be assigned with [databricks_permissions](permissions.md#Instance-Pool-usage) and [instance_pool_id](permissions.md#instance_pool_id) argument",
    )
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host",
    )
    databricks_sql_access: bool | None = Field(
        None,
        description="This is a field to allow the user to have access to [Databricks SQL](https://databricks.com/product/databricks-sql)  UI, [Databricks One](https://docs.databricks.com/aws/en/workspace/databricks-one#who-can-access-databricks-one) and through [databricks_sql_endpoint](sql_endpoint.md)",
    )
    disable_as_user_deletion: bool | None = Field(
        None,
        description="Deactivate the user when deleting the resource, rather than deleting the user entirely. Defaults to `true` when the provider is configured at the account-level and `false` when configured at the workspace-level. This flag is exclusive to force_delete_repos and force_delete_home_dir flags",
    )
    display_name: str | None = Field(
        None,
        description="This is an alias for the username that can be the full name of the user",
    )
    external_id: str | None = Field(
        None, description="ID of the user in an external identity provider"
    )
    force: bool | None = Field(
        None,
        description="Ignore `cannot create user: User with username X already exists` errors and implicitly import the specific user into Terraform state, enforcing entitlements defined in the instance of resource. _This functionality is experimental_ and is designed to simplify corner cases, like Azure Active Directory synchronisation",
    )
    force_delete_home_dir: bool | None = Field(
        None,
        description="This flag determines whether the user's home directory is deleted when the user is deleted. It will have not impact when in the accounts SCIM API. False by default",
    )
    force_delete_repos: bool | None = Field(
        None,
        description="This flag determines whether the user's repo directory is deleted when the user is deleted. It will have no impact when in the accounts SCIM API. False by default",
    )
    home: str | None = Field(
        None, description="Home folder of the user, e.g. `/Users/mr.foo@example.com`"
    )
    repos: str | None = Field(
        None,
        description="Personal Repos location of the user, e.g. `/Repos/mr.foo@example.com`",
    )
    workspace_access: bool | None = Field(
        None,
        description="This is a field to allow the user to have access to a Databricks Workspace UI and [Databricks One](https://docs.databricks.com/aws/en/workspace/databricks-one#who-can-access-databricks-one)",
    )
    workspace_consume: bool | None = Field(
        None,
        description="This is a field to allow the user to have access only to [Databricks One](https://docs.databricks.com/aws/en/workspace/databricks-one#who-can-access-databricks-one).  Couldn't be used with `workspace_access` or `databricks_sql_access`",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_user"
