# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_access_control_rule_set
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class AccessControlRuleSetGrantRules(BaseModel):
    principals: list[str] | None = Field(
        None,
        description="a list of principals who are granted a role. The following format is supported: * `users/{username}` (also exposed as `acl_principal_id` attribute of `databricks_user` resource). * `groups/{groupname}` (also exposed as `acl_principal_id` attribute of `databricks_group` resource). * `servicePrincipals/{applicationId}` (also exposed as `acl_principal_id` attribute of `databricks_service_principal` resource)",
    )
    role: str = Field(
        ...,
        description="Role to be granted. The supported roles are listed below. For more information about these roles, refer to [service principal roles](https://docs.databricks.com/security/auth-authz/access-control/service-principal-acl.html#service-principal-roles), [group roles](https://docs.databricks.com/en/administration-guide/users-groups/groups.html#manage-roles-on-an-account-group-using-the-workspace-admin-settings-page), [marketplace roles](https://docs.databricks.com/en/marketplace/get-started-provider.html#assign-the-marketplace-admin-role) or [budget policy permissions](https://docs.databricks.com/aws/en/admin/usage/budget-policies#manage-budget-policy-permissions), depending on the `name` defined: * `accounts/{account_id}/ruleSets/default`",
    )


class AccessControlRuleSetBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_access_control_rule_set`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    name: str = Field(
        ...,
        description="Unique identifier of a rule set. The name determines the resource to which the rule set applies. **Changing the name recreates the resource!**. Currently, only default rule sets are supported. The following rule set formats are supported:",
    )
    api: str | None = Field(
        None,
        description="Specifies whether to use account-level or workspace-level API. Valid values are `account` and `workspace`. When not set, the API level is inferred from the provider host",
    )
    grant_rules: list[AccessControlRuleSetGrantRules] | None = PluralField(
        None,
        plural="grant_ruless",
        description="The access control rules to be granted by this rule set, consisting of a set of principals and roles to be granted to them",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_access_control_rule_set"
