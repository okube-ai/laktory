# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_account_network_policy
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class AccountNetworkPolicyEgressNetworkAccessAllowedDatabricksDestinations(BaseModel):
    workspace_ids: list[int] | None = Field(
        None, description="The workspace IDs to allow egress traffic to"
    )


class AccountNetworkPolicyEgressNetworkAccessAllowedInternetDestinations(BaseModel):
    destination: str | None = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    internet_destination_type: str | None = Field(
        None,
        description="The type of internet destination. Currently only DNS_NAME is supported. Possible values are: `DNS_NAME`",
    )


class AccountNetworkPolicyEgressNetworkAccessAllowedStorageDestinations(BaseModel):
    azure_storage_account: str | None = Field(
        None, description="The Azure storage account name"
    )
    azure_storage_service: str | None = Field(
        None,
        description="The Azure storage service type (blob, dfs, etc.) * `bucket_name` (string, optional) * `region` (string, optional)",
    )
    bucket_name: str | None = Field(None)
    region: str | None = Field(None)
    storage_destination_type: str | None = Field(
        None,
        description="The type of storage destination. Possible values are: `AWS_S3`, `AZURE_STORAGE`, `GOOGLE_CLOUD_STORAGE`",
    )


class AccountNetworkPolicyEgressNetworkAccessBlockedInternetDestinations(BaseModel):
    destination: str | None = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    internet_destination_type: str | None = Field(
        None,
        description="The type of internet destination. Currently only DNS_NAME is supported. Possible values are: `DNS_NAME`",
    )


class AccountNetworkPolicyEgressNetworkAccessPolicyEnforcement(BaseModel):
    dry_run_mode_product_filter: list[str] | None = Field(
        None,
        description="When empty, it means dry run for all products. When non-empty, it means dry run for specific products and for the other products, they will run in enforced mode",
    )
    enforcement_mode: str | None = Field(
        None,
        description="The mode of policy enforcement. ENFORCED blocks traffic that violates policy, while DRY_RUN only logs violations without blocking. When not specified, defaults to ENFORCED. Possible values are: `DRY_RUN`, `ENFORCED`",
    )


class AccountNetworkPolicyEgressNetworkAccess(BaseModel):
    allowed_databricks_destinations: (
        list[AccountNetworkPolicyEgressNetworkAccessAllowedDatabricksDestinations]
        | None
    ) = Field(
        None,
        description="List of Databricks workspace destinations that serverless workloads are allowed to access when in RESTRICTED_ACCESS mode",
    )
    allowed_internet_destinations: (
        list[AccountNetworkPolicyEgressNetworkAccessAllowedInternetDestinations] | None
    ) = Field(
        None,
        description="List of internet destinations that serverless workloads are allowed to access when in RESTRICTED_ACCESS mode",
    )
    allowed_storage_destinations: (
        list[AccountNetworkPolicyEgressNetworkAccessAllowedStorageDestinations] | None
    ) = Field(
        None,
        description="List of storage destinations that serverless workloads are allowed to access when in RESTRICTED_ACCESS mode",
    )
    blocked_internet_destinations: (
        list[AccountNetworkPolicyEgressNetworkAccessBlockedInternetDestinations] | None
    ) = Field(
        None,
        description="List of internet destinations that serverless workloads are blocked from accessing. These destinations are enforced when restriction mode is RESTRICTED_ACCESS or DRY_RUN. Currently supports DNS_NAME type only; IP_RANGE support is planned",
    )
    policy_enforcement: (
        AccountNetworkPolicyEgressNetworkAccessPolicyEnforcement | None
    ) = Field(
        None,
        description="Optional. When policy_enforcement is not provided, we default to ENFORCE_MODE_ALL_SERVICES",
    )
    restriction_mode: str = Field(
        ...,
        description="The restriction mode that controls how serverless workloads can access the internet. Possible values are: `FULL_ACCESS`, `RESTRICTED_ACCESS`",
    )


class AccountNetworkPolicyEgress(BaseModel):
    network_access: AccountNetworkPolicyEgressNetworkAccess | None = Field(
        None,
        description="The access policy enforced for egress traffic to the internet",
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesAuthentication(
    BaseModel
):
    identities: (
        list[
            AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesAuthenticationIdentities
        ]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAccountApi(
    BaseModel
):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAccountUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestination(BaseModel):
    account_api: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAccountApi
        | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAccountUi
        | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAppsRuntime
        | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationWorkspaceApi
        | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationWorkspaceUi
        | None
    ) = Field(None)


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesOriginSelectedWorkspaces(
    BaseModel
):
    workspace_ids: list[int] | None = Field(
        None, description="The workspace IDs to allow egress traffic to"
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesOrigin(BaseModel):
    all_source_workspaces: bool | None = Field(
        None, description="Matches all source workspaces"
    )
    selected_workspaces: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesOriginSelectedWorkspaces
        | None
    ) = Field(None, description="Specific source workspace IDs to match")


class AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesAuthentication | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestination | None
    ) = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesOrigin | None = (
        Field(
            None,
            description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
        )
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesAuthentication(BaseModel):
    identities: (
        list[
            AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesAuthenticationIdentities
        ]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAccountApi(
    BaseModel
):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAccountUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestination(BaseModel):
    account_api: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAccountApi
        | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAccountUi
        | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAppsRuntime
        | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationWorkspaceApi
        | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationWorkspaceUi
        | None
    ) = Field(None)


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesOriginSelectedWorkspaces(
    BaseModel
):
    workspace_ids: list[int] | None = Field(
        None, description="The workspace IDs to allow egress traffic to"
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesOrigin(BaseModel):
    all_source_workspaces: bool | None = Field(
        None, description="Matches all source workspaces"
    )
    selected_workspaces: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesOriginSelectedWorkspaces
        | None
    ) = Field(None, description="Specific source workspace IDs to match")


class AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesAuthentication | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: (
        AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestination | None
    ) = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesOrigin | None = (
        Field(
            None,
            description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
        )
    )


class AccountNetworkPolicyIngressCrossWorkspaceAccess(BaseModel):
    allow_rules: (
        list[AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRules] | None
    ) = Field(
        None,
        description="Allow rules are evaluated after deny rules. A request matching any allow rule is allowed; a request matching no rule is denied by default. Only applies when restriction_mode is RESTRICTED_ACCESS",
    )
    deny_rules: (
        list[AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRules] | None
    ) = Field(
        None,
        description="Deny rules are evaluated first. A request matching any deny rule is denied, regardless of allow rules. Only applies when restriction_mode is RESTRICTED_ACCESS",
    )
    restriction_mode: str = Field(
        ...,
        description="The restriction mode that controls how serverless workloads can access the internet. Possible values are: `FULL_ACCESS`, `RESTRICTED_ACCESS`",
    )


class AccountNetworkPolicyIngressPrivateAccessAllowRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressPrivateAccessAllowRulesAuthentication(BaseModel):
    identities: (
        list[AccountNetworkPolicyIngressPrivateAccessAllowRulesAuthenticationIdentities]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAccountApi(
    BaseModel
):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAccountUi(BaseModel):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPrivateAccessAllowRulesDestination(BaseModel):
    account_api: (
        AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAccountApi | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAccountUi | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAppsRuntime | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationWorkspaceApi | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationWorkspaceUi | None
    ) = Field(None)


class AccountNetworkPolicyIngressPrivateAccessAllowRulesOriginEndpoints(BaseModel):
    endpoint_ids: list[str] | None = Field(
        None,
        description="The IDs of the registered endpoints. Must contain at least one endpoint ID",
    )


class AccountNetworkPolicyIngressPrivateAccessAllowRulesOrigin(BaseModel):
    all_private_access: bool | None = Field(
        None,
        description="Matches requests arriving over any private connectivity, including registered endpoints and the workspace's Azure Private Link (ui-api) endpoints. Can only be used in deny rules of workspace-level network policies. Must be set to true when specified",
    )
    all_registered_endpoints: bool | None = Field(
        None,
        description="Matches requests arriving through any endpoint registered to the account. Must be set to true when specified",
    )
    azure_workspace_private_link: bool | None = Field(
        None,
        description="Matches requests arriving through the workspace's Azure Private Link (ui-api) endpoints. Can only be used in deny rules of workspace-level network policies. Must be set to true when specified",
    )
    endpoints: (
        AccountNetworkPolicyIngressPrivateAccessAllowRulesOriginEndpoints | None
    ) = Field(
        None,
        description="Matches requests arriving through any of the specified registered endpoints",
    )


class AccountNetworkPolicyIngressPrivateAccessAllowRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressPrivateAccessAllowRulesAuthentication | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: (
        AccountNetworkPolicyIngressPrivateAccessAllowRulesDestination | None
    ) = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: AccountNetworkPolicyIngressPrivateAccessAllowRulesOrigin | None = Field(
        None,
        description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
    )


class AccountNetworkPolicyIngressPrivateAccessDenyRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressPrivateAccessDenyRulesAuthentication(BaseModel):
    identities: (
        list[AccountNetworkPolicyIngressPrivateAccessDenyRulesAuthenticationIdentities]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAccountApi(BaseModel):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAccountUi(BaseModel):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPrivateAccessDenyRulesDestination(BaseModel):
    account_api: (
        AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAccountApi | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAccountUi | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAppsRuntime | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationWorkspaceApi | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationWorkspaceUi | None
    ) = Field(None)


class AccountNetworkPolicyIngressPrivateAccessDenyRulesOriginEndpoints(BaseModel):
    endpoint_ids: list[str] | None = Field(
        None,
        description="The IDs of the registered endpoints. Must contain at least one endpoint ID",
    )


class AccountNetworkPolicyIngressPrivateAccessDenyRulesOrigin(BaseModel):
    all_private_access: bool | None = Field(
        None,
        description="Matches requests arriving over any private connectivity, including registered endpoints and the workspace's Azure Private Link (ui-api) endpoints. Can only be used in deny rules of workspace-level network policies. Must be set to true when specified",
    )
    all_registered_endpoints: bool | None = Field(
        None,
        description="Matches requests arriving through any endpoint registered to the account. Must be set to true when specified",
    )
    azure_workspace_private_link: bool | None = Field(
        None,
        description="Matches requests arriving through the workspace's Azure Private Link (ui-api) endpoints. Can only be used in deny rules of workspace-level network policies. Must be set to true when specified",
    )
    endpoints: (
        AccountNetworkPolicyIngressPrivateAccessDenyRulesOriginEndpoints | None
    ) = Field(
        None,
        description="Matches requests arriving through any of the specified registered endpoints",
    )


class AccountNetworkPolicyIngressPrivateAccessDenyRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressPrivateAccessDenyRulesAuthentication | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: AccountNetworkPolicyIngressPrivateAccessDenyRulesDestination | None = (
        Field(
            None,
            description="The internet destination to which access will be allowed. Format dependent on the destination type",
        )
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: AccountNetworkPolicyIngressPrivateAccessDenyRulesOrigin | None = Field(
        None,
        description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
    )


class AccountNetworkPolicyIngressPrivateAccess(BaseModel):
    allow_rules: list[AccountNetworkPolicyIngressPrivateAccessAllowRules] | None = (
        Field(
            None,
            description="Allow rules are evaluated after deny rules. A request matching any allow rule is allowed; a request matching no rule is denied by default. Only applies when restriction_mode is RESTRICTED_ACCESS",
        )
    )
    deny_rules: list[AccountNetworkPolicyIngressPrivateAccessDenyRules] | None = Field(
        None,
        description="Deny rules are evaluated first. A request matching any deny rule is denied, regardless of allow rules. Only applies when restriction_mode is RESTRICTED_ACCESS",
    )
    restriction_mode: str = Field(
        ...,
        description="The restriction mode that controls how serverless workloads can access the internet. Possible values are: `FULL_ACCESS`, `RESTRICTED_ACCESS`",
    )


class AccountNetworkPolicyIngressPublicAccessAllowRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressPublicAccessAllowRulesAuthentication(BaseModel):
    identities: (
        list[AccountNetworkPolicyIngressPublicAccessAllowRulesAuthenticationIdentities]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAccountApi(BaseModel):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAccountUi(BaseModel):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPublicAccessAllowRulesDestination(BaseModel):
    account_api: (
        AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAccountApi | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAccountUi | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAppsRuntime | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationWorkspaceApi | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationWorkspaceUi | None
    ) = Field(None)


class AccountNetworkPolicyIngressPublicAccessAllowRulesOriginExcludedIpRanges(
    BaseModel
):
    ip_ranges: list[str] | None = Field(
        None, description="We only support IPv4 and IPv4 CIDR notation for now"
    )


class AccountNetworkPolicyIngressPublicAccessAllowRulesOriginIncludedIpRanges(
    BaseModel
):
    ip_ranges: list[str] | None = Field(
        None, description="We only support IPv4 and IPv4 CIDR notation for now"
    )


class AccountNetworkPolicyIngressPublicAccessAllowRulesOrigin(BaseModel):
    all_ip_ranges: bool | None = Field(
        None, description="Matches all IPv4 and IPv6 ranges (both public and private)"
    )
    excluded_ip_ranges: (
        AccountNetworkPolicyIngressPublicAccessAllowRulesOriginExcludedIpRanges | None
    ) = Field(None, description="Excluded means: all public IP ranges except this one")
    included_ip_ranges: (
        AccountNetworkPolicyIngressPublicAccessAllowRulesOriginIncludedIpRanges | None
    ) = Field(None, description="Will not allow IP ranges with private IPs")


class AccountNetworkPolicyIngressPublicAccessAllowRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressPublicAccessAllowRulesAuthentication | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: AccountNetworkPolicyIngressPublicAccessAllowRulesDestination | None = (
        Field(
            None,
            description="The internet destination to which access will be allowed. Format dependent on the destination type",
        )
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: AccountNetworkPolicyIngressPublicAccessAllowRulesOrigin | None = Field(
        None,
        description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
    )


class AccountNetworkPolicyIngressPublicAccessDenyRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressPublicAccessDenyRulesAuthentication(BaseModel):
    identities: (
        list[AccountNetworkPolicyIngressPublicAccessDenyRulesAuthenticationIdentities]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAccountApi(BaseModel):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAccountUi(BaseModel):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAppsRuntime(BaseModel):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationWorkspaceUi(BaseModel):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressPublicAccessDenyRulesDestination(BaseModel):
    account_api: (
        AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAccountApi | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAccountUi | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAppsRuntime | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationWorkspaceApi | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationWorkspaceUi | None
    ) = Field(None)


class AccountNetworkPolicyIngressPublicAccessDenyRulesOriginExcludedIpRanges(BaseModel):
    ip_ranges: list[str] | None = Field(
        None, description="We only support IPv4 and IPv4 CIDR notation for now"
    )


class AccountNetworkPolicyIngressPublicAccessDenyRulesOriginIncludedIpRanges(BaseModel):
    ip_ranges: list[str] | None = Field(
        None, description="We only support IPv4 and IPv4 CIDR notation for now"
    )


class AccountNetworkPolicyIngressPublicAccessDenyRulesOrigin(BaseModel):
    all_ip_ranges: bool | None = Field(
        None, description="Matches all IPv4 and IPv6 ranges (both public and private)"
    )
    excluded_ip_ranges: (
        AccountNetworkPolicyIngressPublicAccessDenyRulesOriginExcludedIpRanges | None
    ) = Field(None, description="Excluded means: all public IP ranges except this one")
    included_ip_ranges: (
        AccountNetworkPolicyIngressPublicAccessDenyRulesOriginIncludedIpRanges | None
    ) = Field(None, description="Will not allow IP ranges with private IPs")


class AccountNetworkPolicyIngressPublicAccessDenyRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressPublicAccessDenyRulesAuthentication | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: AccountNetworkPolicyIngressPublicAccessDenyRulesDestination | None = (
        Field(
            None,
            description="The internet destination to which access will be allowed. Format dependent on the destination type",
        )
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: AccountNetworkPolicyIngressPublicAccessDenyRulesOrigin | None = Field(
        None,
        description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
    )


class AccountNetworkPolicyIngressPublicAccess(BaseModel):
    allow_rules: list[AccountNetworkPolicyIngressPublicAccessAllowRules] | None = Field(
        None,
        description="Allow rules are evaluated after deny rules. A request matching any allow rule is allowed; a request matching no rule is denied by default. Only applies when restriction_mode is RESTRICTED_ACCESS",
    )
    deny_rules: list[AccountNetworkPolicyIngressPublicAccessDenyRules] | None = Field(
        None,
        description="Deny rules are evaluated first. A request matching any deny rule is denied, regardless of allow rules. Only applies when restriction_mode is RESTRICTED_ACCESS",
    )
    restriction_mode: str = Field(
        ...,
        description="The restriction mode that controls how serverless workloads can access the internet. Possible values are: `FULL_ACCESS`, `RESTRICTED_ACCESS`",
    )


class AccountNetworkPolicyIngress(BaseModel):
    cross_workspace_access: AccountNetworkPolicyIngressCrossWorkspaceAccess | None = (
        Field(None)
    )
    private_access: AccountNetworkPolicyIngressPrivateAccess | None = Field(
        None,
        description="The network policy restrictions for private access. Configures how requests arriving over private connectivity are governed",
    )
    public_access: AccountNetworkPolicyIngressPublicAccess | None = Field(
        None,
        description="The network policy restrictions for public access to the workspace. Configures how public internet traffic is allowed or denied access",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesAuthentication(
    BaseModel
):
    identities: (
        list[
            AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesAuthenticationIdentities
        ]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAccountApi(
    BaseModel
):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAccountUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestination(
    BaseModel
):
    account_api: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAccountApi
        | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAccountUi
        | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAppsRuntime
        | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationWorkspaceApi
        | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationWorkspaceUi
        | None
    ) = Field(None)


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesOriginSelectedWorkspaces(
    BaseModel
):
    workspace_ids: list[int] | None = Field(
        None, description="The workspace IDs to allow egress traffic to"
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesOrigin(BaseModel):
    all_source_workspaces: bool | None = Field(
        None, description="Matches all source workspaces"
    )
    selected_workspaces: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesOriginSelectedWorkspaces
        | None
    ) = Field(None, description="Specific source workspace IDs to match")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesAuthentication
        | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestination
        | None
    ) = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesOrigin | None
    ) = Field(
        None,
        description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesAuthentication(
    BaseModel
):
    identities: (
        list[
            AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesAuthenticationIdentities
        ]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAccountApi(
    BaseModel
):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAccountUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestination(
    BaseModel
):
    account_api: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAccountApi
        | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAccountUi
        | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAppsRuntime
        | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationWorkspaceApi
        | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationWorkspaceUi
        | None
    ) = Field(None)


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesOriginSelectedWorkspaces(
    BaseModel
):
    workspace_ids: list[int] | None = Field(
        None, description="The workspace IDs to allow egress traffic to"
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesOrigin(BaseModel):
    all_source_workspaces: bool | None = Field(
        None, description="Matches all source workspaces"
    )
    selected_workspaces: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesOriginSelectedWorkspaces
        | None
    ) = Field(None, description="Specific source workspace IDs to match")


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesAuthentication
        | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestination | None
    ) = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesOrigin | None
    ) = Field(
        None,
        description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
    )


class AccountNetworkPolicyIngressDryRunCrossWorkspaceAccess(BaseModel):
    allow_rules: (
        list[AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRules] | None
    ) = Field(
        None,
        description="Allow rules are evaluated after deny rules. A request matching any allow rule is allowed; a request matching no rule is denied by default. Only applies when restriction_mode is RESTRICTED_ACCESS",
    )
    deny_rules: (
        list[AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRules] | None
    ) = Field(
        None,
        description="Deny rules are evaluated first. A request matching any deny rule is denied, regardless of allow rules. Only applies when restriction_mode is RESTRICTED_ACCESS",
    )
    restriction_mode: str = Field(
        ...,
        description="The restriction mode that controls how serverless workloads can access the internet. Possible values are: `FULL_ACCESS`, `RESTRICTED_ACCESS`",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesAuthentication(BaseModel):
    identities: (
        list[
            AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesAuthenticationIdentities
        ]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAccountApi(
    BaseModel
):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAccountUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestination(BaseModel):
    account_api: (
        AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAccountApi
        | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAccountUi
        | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAppsRuntime
        | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationWorkspaceApi
        | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationWorkspaceUi
        | None
    ) = Field(None)


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesOriginEndpoints(
    BaseModel
):
    endpoint_ids: list[str] | None = Field(
        None,
        description="The IDs of the registered endpoints. Must contain at least one endpoint ID",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesOrigin(BaseModel):
    all_private_access: bool | None = Field(
        None,
        description="Matches requests arriving over any private connectivity, including registered endpoints and the workspace's Azure Private Link (ui-api) endpoints. Can only be used in deny rules of workspace-level network policies. Must be set to true when specified",
    )
    all_registered_endpoints: bool | None = Field(
        None,
        description="Matches requests arriving through any endpoint registered to the account. Must be set to true when specified",
    )
    azure_workspace_private_link: bool | None = Field(
        None,
        description="Matches requests arriving through the workspace's Azure Private Link (ui-api) endpoints. Can only be used in deny rules of workspace-level network policies. Must be set to true when specified",
    )
    endpoints: (
        AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesOriginEndpoints | None
    ) = Field(
        None,
        description="Matches requests arriving through any of the specified registered endpoints",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessAllowRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesAuthentication | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: (
        AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestination | None
    ) = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesOrigin | None = (
        Field(
            None,
            description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
        )
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesAuthentication(BaseModel):
    identities: (
        list[
            AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesAuthenticationIdentities
        ]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAccountApi(
    BaseModel
):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAccountUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestination(BaseModel):
    account_api: (
        AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAccountApi
        | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAccountUi
        | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAppsRuntime
        | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationWorkspaceApi
        | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationWorkspaceUi
        | None
    ) = Field(None)


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesOriginEndpoints(BaseModel):
    endpoint_ids: list[str] | None = Field(
        None,
        description="The IDs of the registered endpoints. Must contain at least one endpoint ID",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesOrigin(BaseModel):
    all_private_access: bool | None = Field(
        None,
        description="Matches requests arriving over any private connectivity, including registered endpoints and the workspace's Azure Private Link (ui-api) endpoints. Can only be used in deny rules of workspace-level network policies. Must be set to true when specified",
    )
    all_registered_endpoints: bool | None = Field(
        None,
        description="Matches requests arriving through any endpoint registered to the account. Must be set to true when specified",
    )
    azure_workspace_private_link: bool | None = Field(
        None,
        description="Matches requests arriving through the workspace's Azure Private Link (ui-api) endpoints. Can only be used in deny rules of workspace-level network policies. Must be set to true when specified",
    )
    endpoints: (
        AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesOriginEndpoints | None
    ) = Field(
        None,
        description="Matches requests arriving through any of the specified registered endpoints",
    )


class AccountNetworkPolicyIngressDryRunPrivateAccessDenyRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesAuthentication | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: (
        AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestination | None
    ) = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesOrigin | None = (
        Field(
            None,
            description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
        )
    )


class AccountNetworkPolicyIngressDryRunPrivateAccess(BaseModel):
    allow_rules: (
        list[AccountNetworkPolicyIngressDryRunPrivateAccessAllowRules] | None
    ) = Field(
        None,
        description="Allow rules are evaluated after deny rules. A request matching any allow rule is allowed; a request matching no rule is denied by default. Only applies when restriction_mode is RESTRICTED_ACCESS",
    )
    deny_rules: list[AccountNetworkPolicyIngressDryRunPrivateAccessDenyRules] | None = (
        Field(
            None,
            description="Deny rules are evaluated first. A request matching any deny rule is denied, regardless of allow rules. Only applies when restriction_mode is RESTRICTED_ACCESS",
        )
    )
    restriction_mode: str = Field(
        ...,
        description="The restriction mode that controls how serverless workloads can access the internet. Possible values are: `FULL_ACCESS`, `RESTRICTED_ACCESS`",
    )


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesAuthentication(BaseModel):
    identities: (
        list[
            AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesAuthenticationIdentities
        ]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAccountApi(
    BaseModel
):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAccountUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestination(BaseModel):
    account_api: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAccountApi
        | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAccountUi
        | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAppsRuntime
        | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationWorkspaceApi
        | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationWorkspaceUi
        | None
    ) = Field(None)


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesOriginExcludedIpRanges(
    BaseModel
):
    ip_ranges: list[str] | None = Field(
        None, description="We only support IPv4 and IPv4 CIDR notation for now"
    )


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesOriginIncludedIpRanges(
    BaseModel
):
    ip_ranges: list[str] | None = Field(
        None, description="We only support IPv4 and IPv4 CIDR notation for now"
    )


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesOrigin(BaseModel):
    all_ip_ranges: bool | None = Field(
        None, description="Matches all IPv4 and IPv6 ranges (both public and private)"
    )
    excluded_ip_ranges: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesOriginExcludedIpRanges
        | None
    ) = Field(None, description="Excluded means: all public IP ranges except this one")
    included_ip_ranges: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesOriginIncludedIpRanges
        | None
    ) = Field(None, description="Will not allow IP ranges with private IPs")


class AccountNetworkPolicyIngressDryRunPublicAccessAllowRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesAuthentication | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: (
        AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestination | None
    ) = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesOrigin | None = (
        Field(
            None,
            description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
        )
    )


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesAuthenticationIdentities(
    BaseModel
):
    principal_id: int | None = Field(None)
    principal_type: str | None = Field(
        None,
        description="Possible values are: `PRINCIPAL_TYPE_SERVICE_PRINCIPAL`, `PRINCIPAL_TYPE_USER`",
    )


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesAuthentication(BaseModel):
    identities: (
        list[
            AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesAuthenticationIdentities
        ]
        | None
    ) = Field(
        None,
        description="Valid only when IdentityType is IDENTITY_TYPE_SELECTED_IDENTITIES",
    )
    identity_type: str | None = Field(
        None,
        description="Possible values are: `IDENTITY_TYPE_ALL_SERVICE_PRINCIPALS`, `IDENTITY_TYPE_ALL_USERS`, `IDENTITY_TYPE_SELECTED_IDENTITIES`",
    )


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAccountApi(
    BaseModel
):
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAccountDatabricksOne(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAccountUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAppsRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationLakebaseRuntime(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationWorkspaceApi(
    BaseModel
):
    excluded_scopes: list[str] | None = Field(
        None,
        description="Inverse of `scopes`: matches every API scope EXCEPT those listed here ('allow all except'). Mutually exclusive with `scopes` — a single destination may set at most one of the two",
    )
    scope_qualifier: str | None = Field(
        None,
        description="Qualifies the breadth of API access for the listed scopes. See ApiScopeQualifier. Possible values are: `API_SCOPE_QUALIFIER_ALL`, `API_SCOPE_QUALIFIER_READ` * `scopes` (list of string, optional)",
    )
    scopes: list[str] | None = Field(
        None,
        description="The API scopes to match. Use 'all-apis' to match any account-level API",
    )


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationWorkspaceUi(
    BaseModel
):
    all_destinations: bool | None = Field(None, description="Must be set to true")


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestination(BaseModel):
    account_api: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAccountApi
        | None
    ) = Field(
        None,
        description="Matches requests to account-level APIs. Can only be used in the account-level network policy",
    )
    account_databricks_one: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAccountDatabricksOne
        | None
    ) = Field(None, description="Account DatabricksOne destination is not supported")
    account_ui: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAccountUi
        | None
    ) = Field(
        None,
        description="Matches requests to the account console UI. Can only be used in the account-level network policy",
    )
    all_destinations: bool | None = Field(None, description="Must be set to true")
    apps_runtime: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAppsRuntime
        | None
    ) = Field(None)
    lakebase_runtime: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationLakebaseRuntime
        | None
    ) = Field(None)
    workspace_api: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationWorkspaceApi
        | None
    ) = Field(None)
    workspace_ui: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationWorkspaceUi
        | None
    ) = Field(None)


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesOriginExcludedIpRanges(
    BaseModel
):
    ip_ranges: list[str] | None = Field(
        None, description="We only support IPv4 and IPv4 CIDR notation for now"
    )


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesOriginIncludedIpRanges(
    BaseModel
):
    ip_ranges: list[str] | None = Field(
        None, description="We only support IPv4 and IPv4 CIDR notation for now"
    )


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesOrigin(BaseModel):
    all_ip_ranges: bool | None = Field(
        None, description="Matches all IPv4 and IPv6 ranges (both public and private)"
    )
    excluded_ip_ranges: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesOriginExcludedIpRanges
        | None
    ) = Field(None, description="Excluded means: all public IP ranges except this one")
    included_ip_ranges: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesOriginIncludedIpRanges
        | None
    ) = Field(None, description="Will not allow IP ranges with private IPs")


class AccountNetworkPolicyIngressDryRunPublicAccessDenyRules(BaseModel):
    authentication: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesAuthentication | None
    ) = Field(
        None,
        description="The authenticated identity the request must match. When unset, the rule matches all users and service principals. On the account-level network policy, scoping to specific identities is not currently supported, so this field must be unset (the rule matches all users and service principals)",
    )
    destination: (
        AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestination | None
    ) = Field(
        None,
        description="The internet destination to which access will be allowed. Format dependent on the destination type",
    )
    label: str | None = Field(
        None,
        description="The label for this ingress rule * `origin` (CustomerFacingIngressNetworkPolicyPublicRequestOrigin, optional)",
    )
    origin: AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesOrigin | None = Field(
        None,
        description="The origin the request must match — the private connectivity the request arrives through, for example a specific set of registered endpoints or any endpoint registered to the account. See PrivateRequestOrigin",
    )


class AccountNetworkPolicyIngressDryRunPublicAccess(BaseModel):
    allow_rules: (
        list[AccountNetworkPolicyIngressDryRunPublicAccessAllowRules] | None
    ) = Field(
        None,
        description="Allow rules are evaluated after deny rules. A request matching any allow rule is allowed; a request matching no rule is denied by default. Only applies when restriction_mode is RESTRICTED_ACCESS",
    )
    deny_rules: list[AccountNetworkPolicyIngressDryRunPublicAccessDenyRules] | None = (
        Field(
            None,
            description="Deny rules are evaluated first. A request matching any deny rule is denied, regardless of allow rules. Only applies when restriction_mode is RESTRICTED_ACCESS",
        )
    )
    restriction_mode: str = Field(
        ...,
        description="The restriction mode that controls how serverless workloads can access the internet. Possible values are: `FULL_ACCESS`, `RESTRICTED_ACCESS`",
    )


class AccountNetworkPolicyIngressDryRun(BaseModel):
    cross_workspace_access: (
        AccountNetworkPolicyIngressDryRunCrossWorkspaceAccess | None
    ) = Field(None)
    private_access: AccountNetworkPolicyIngressDryRunPrivateAccess | None = Field(
        None,
        description="The network policy restrictions for private access. Configures how requests arriving over private connectivity are governed",
    )
    public_access: AccountNetworkPolicyIngressDryRunPublicAccess | None = Field(
        None,
        description="The network policy restrictions for public access to the workspace. Configures how public internet traffic is allowed or denied access",
    )


class AccountNetworkPolicyBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_account_network_policy`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    account_id: str | None = Field(
        None, description="The associated account ID for this Network Policy object"
    )
    network_policy_id: str | None = Field(
        None, description="The unique identifier for the network policy"
    )
    egress: AccountNetworkPolicyEgress | None = Field(
        None, description="The network policies applying for egress traffic"
    )
    ingress: AccountNetworkPolicyIngress | None = Field(
        None, description="The network policies applying for ingress traffic"
    )
    ingress_dry_run: AccountNetworkPolicyIngressDryRun | None = Field(
        None,
        description="The ingress policy for dry run mode. Dry run will always run even if the request is allowed by the ingress policy. When this field is set, the policy will be evaluated and emit logs only without blocking requests",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_account_network_policy"


__all__ = [
    "AccountNetworkPolicyBase",
    "AccountNetworkPolicyEgress",
    "AccountNetworkPolicyEgressNetworkAccess",
    "AccountNetworkPolicyEgressNetworkAccessAllowedDatabricksDestinations",
    "AccountNetworkPolicyEgressNetworkAccessAllowedInternetDestinations",
    "AccountNetworkPolicyEgressNetworkAccessAllowedStorageDestinations",
    "AccountNetworkPolicyEgressNetworkAccessBlockedInternetDestinations",
    "AccountNetworkPolicyEgressNetworkAccessPolicyEnforcement",
    "AccountNetworkPolicyIngress",
    "AccountNetworkPolicyIngressCrossWorkspaceAccess",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRules",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesAuthentication",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestination",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesOrigin",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessAllowRulesOriginSelectedWorkspaces",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRules",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesAuthentication",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestination",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesOrigin",
    "AccountNetworkPolicyIngressCrossWorkspaceAccessDenyRulesOriginSelectedWorkspaces",
    "AccountNetworkPolicyIngressDryRun",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccess",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRules",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesAuthentication",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestination",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesOrigin",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessAllowRulesOriginSelectedWorkspaces",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRules",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesAuthentication",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestination",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesOrigin",
    "AccountNetworkPolicyIngressDryRunCrossWorkspaceAccessDenyRulesOriginSelectedWorkspaces",
    "AccountNetworkPolicyIngressDryRunPrivateAccess",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRules",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesAuthentication",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestination",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesOrigin",
    "AccountNetworkPolicyIngressDryRunPrivateAccessAllowRulesOriginEndpoints",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRules",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesAuthentication",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestination",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesOrigin",
    "AccountNetworkPolicyIngressDryRunPrivateAccessDenyRulesOriginEndpoints",
    "AccountNetworkPolicyIngressDryRunPublicAccess",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRules",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesAuthentication",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestination",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesOrigin",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesOriginExcludedIpRanges",
    "AccountNetworkPolicyIngressDryRunPublicAccessAllowRulesOriginIncludedIpRanges",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRules",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesAuthentication",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestination",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesOrigin",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesOriginExcludedIpRanges",
    "AccountNetworkPolicyIngressDryRunPublicAccessDenyRulesOriginIncludedIpRanges",
    "AccountNetworkPolicyIngressPrivateAccess",
    "AccountNetworkPolicyIngressPrivateAccessAllowRules",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesAuthentication",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesDestination",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesOrigin",
    "AccountNetworkPolicyIngressPrivateAccessAllowRulesOriginEndpoints",
    "AccountNetworkPolicyIngressPrivateAccessDenyRules",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesAuthentication",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesDestination",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesOrigin",
    "AccountNetworkPolicyIngressPrivateAccessDenyRulesOriginEndpoints",
    "AccountNetworkPolicyIngressPublicAccess",
    "AccountNetworkPolicyIngressPublicAccessAllowRules",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesAuthentication",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesDestination",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesOrigin",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesOriginExcludedIpRanges",
    "AccountNetworkPolicyIngressPublicAccessAllowRulesOriginIncludedIpRanges",
    "AccountNetworkPolicyIngressPublicAccessDenyRules",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesAuthentication",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesAuthenticationIdentities",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesDestination",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAccountApi",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAccountDatabricksOne",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAccountUi",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationAppsRuntime",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationLakebaseRuntime",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationWorkspaceApi",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesDestinationWorkspaceUi",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesOrigin",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesOriginExcludedIpRanges",
    "AccountNetworkPolicyIngressPublicAccessDenyRulesOriginIncludedIpRanges",
]
