# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_policy_info
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class PolicyInfoColumnMaskUsingFunctionArgExpressionTagIntrospectionColumnTagValue(
    BaseModel
):
    column_alias: str = Field(
        ..., description="The alias from MATCH COLUMNS that identifies the column"
    )
    tag_key: str = Field(
        ...,
        description="1024 matches the max_length on FunctionArgument.constant above",
    )


class PolicyInfoColumnMaskUsingFunctionArgExpressionTagIntrospectionTagValue(BaseModel):
    tag_key: str = Field(
        ...,
        description="1024 matches the max_length on FunctionArgument.constant above",
    )


class PolicyInfoColumnMaskUsingFunctionArgExpressionTagIntrospection(BaseModel):
    column_tag_value: (
        PolicyInfoColumnMaskUsingFunctionArgExpressionTagIntrospectionColumnTagValue
        | None
    ) = Field(None, description="Extracts the value of a column-level tag")
    tag_value: (
        PolicyInfoColumnMaskUsingFunctionArgExpressionTagIntrospectionTagValue | None
    ) = Field(None, description="Extracts the value of a securable-level tag")


class PolicyInfoColumnMaskUsingFunctionArgExpression(BaseModel):
    tag_introspection: (
        PolicyInfoColumnMaskUsingFunctionArgExpressionTagIntrospection | None
    ) = Field(None, description="An expression that introspects tags at query time")


class PolicyInfoColumnMaskUsing(BaseModel):
    alias: str | None = Field(None, description="Optional alias of the matched column")
    constant: str | None = Field(None, description="A constant literal")
    function_arg_expression: PolicyInfoColumnMaskUsingFunctionArgExpression | None = (
        Field(
            None,
            description="An expression evaluated at query time. Wraps per-request expression variants (e.g., tag introspection) so new variants can be added without extending the FunctionArgument oneof",
        )
    )


class PolicyInfoColumnMask(BaseModel):
    function_name: str = Field(
        ...,
        description="The fully qualified name of the row filter function. The function is called on each row of the target table. It should return a boolean value indicating whether the row should be visible to the user. Required on create and update",
    )
    on_column: str = Field(
        ...,
        description="The alias of the column to be masked. The alias must refer to one of matched columns. The values of the column is passed to the column mask function as the first argument. Required on create and update",
    )
    using: list[PolicyInfoColumnMaskUsing] | None = PluralField(
        None,
        plural="usings",
        description="Optional list of column aliases or constant literals to be passed as arguments to the row filter function. The type of each column should match the positional argument of the row filter function",
    )


class PolicyInfoDeny(BaseModel):
    privileges: list[str] = Field(
        ...,
        description="List of privileges to grant. When any of these privileges are requested, the policy will grant access if the principal and condition match. Required on create and update",
    )


class PolicyInfoGrant(BaseModel):
    privileges: list[str] = Field(
        ...,
        description="List of privileges to grant. When any of these privileges are requested, the policy will grant access if the principal and condition match. Required on create and update",
    )


class PolicyInfoMatchColumns(BaseModel):
    alias: str | None = Field(None, description="Optional alias of the matched column")
    condition: str | None = Field(
        None, description="The condition expression used to match a table column"
    )


class PolicyInfoProviderConfig(BaseModel):
    workspace_id: str | None = Field(
        None,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with",
    )


class PolicyInfoRowFilterUsingFunctionArgExpressionTagIntrospectionColumnTagValue(
    BaseModel
):
    column_alias: str = Field(
        ..., description="The alias from MATCH COLUMNS that identifies the column"
    )
    tag_key: str = Field(
        ...,
        description="1024 matches the max_length on FunctionArgument.constant above",
    )


class PolicyInfoRowFilterUsingFunctionArgExpressionTagIntrospectionTagValue(BaseModel):
    tag_key: str = Field(
        ...,
        description="1024 matches the max_length on FunctionArgument.constant above",
    )


class PolicyInfoRowFilterUsingFunctionArgExpressionTagIntrospection(BaseModel):
    column_tag_value: (
        PolicyInfoRowFilterUsingFunctionArgExpressionTagIntrospectionColumnTagValue
        | None
    ) = Field(None, description="Extracts the value of a column-level tag")
    tag_value: (
        PolicyInfoRowFilterUsingFunctionArgExpressionTagIntrospectionTagValue | None
    ) = Field(None, description="Extracts the value of a securable-level tag")


class PolicyInfoRowFilterUsingFunctionArgExpression(BaseModel):
    tag_introspection: (
        PolicyInfoRowFilterUsingFunctionArgExpressionTagIntrospection | None
    ) = Field(None, description="An expression that introspects tags at query time")


class PolicyInfoRowFilterUsing(BaseModel):
    alias: str | None = Field(None, description="Optional alias of the matched column")
    constant: str | None = Field(None, description="A constant literal")
    function_arg_expression: PolicyInfoRowFilterUsingFunctionArgExpression | None = (
        Field(
            None,
            description="An expression evaluated at query time. Wraps per-request expression variants (e.g., tag introspection) so new variants can be added without extending the FunctionArgument oneof",
        )
    )


class PolicyInfoRowFilter(BaseModel):
    function_name: str = Field(
        ...,
        description="The fully qualified name of the row filter function. The function is called on each row of the target table. It should return a boolean value indicating whether the row should be visible to the user. Required on create and update",
    )
    using: list[PolicyInfoRowFilterUsing] | None = PluralField(
        None,
        plural="usings",
        description="Optional list of column aliases or constant literals to be passed as arguments to the row filter function. The type of each column should match the positional argument of the row filter function",
    )


class PolicyInfoBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_policy_info`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    for_securable_type: str = Field(
        ...,
        description="Type of securables that the policy should take effect on. Required on create and optional on update. Possible values are: `AGENT_SERVICE`, `CATALOG`, `CLEAN_ROOM`, `CONNECTION`, `CREDENTIAL`, `EXTERNAL_LOCATION`, `EXTERNAL_METADATA`, `FUNCTION`, `MCP_SERVICE`, `METASTORE`, `MODEL`, `MODEL_PROVIDER_SERVICE`, `MODEL_SERVICE`, `PIPELINE`, `PROVIDER`, `RECIPIENT`, `SCHEMA`, `SHARE`, `SKILL`, `STAGING_TABLE`, `STORAGE_CREDENTIAL`, `TABLE`, `VOLUME`",
    )
    policy_type: str = Field(
        ...,
        description="Type of the policy. Required on create. Possible values are: `POLICY_TYPE_COLUMN_MASK`, `POLICY_TYPE_DENY`, `POLICY_TYPE_GRANT`, `POLICY_TYPE_ROW_FILTER`",
    )
    to_principals: list[str] = Field(
        ...,
        description="List of user or group names that the policy applies to. Required on create and optional on update",
    )
    comment: str | None = Field(None, description="Optional description of the policy")
    except_principals: list[str] | None = Field(
        None,
        description="Optional list of user or group names that should be excluded from the policy",
    )
    name: str | None = Field(
        None,
        description="Name of the policy. Required on create and optional on update. To rename the policy, set `name` to a different value on update",
    )
    on_securable_fullname: str | None = Field(
        None,
        description="Full name of the securable on which the policy is defined. Required on create",
    )
    on_securable_type: str | None = Field(
        None,
        description="Type of the securable on which the policy is defined. Only `CATALOG`, `SCHEMA` and `TABLE` are supported at this moment. Required on create. Possible values are: `AGENT_SERVICE`, `CATALOG`, `CLEAN_ROOM`, `CONNECTION`, `CREDENTIAL`, `EXTERNAL_LOCATION`, `EXTERNAL_METADATA`, `FUNCTION`, `MCP_SERVICE`, `METASTORE`, `MODEL`, `MODEL_PROVIDER_SERVICE`, `MODEL_SERVICE`, `PIPELINE`, `PROVIDER`, `RECIPIENT`, `SCHEMA`, `SHARE`, `SKILL`, `STAGING_TABLE`, `STORAGE_CREDENTIAL`, `TABLE`, `VOLUME`",
    )
    when_condition: str | None = Field(
        None, description="Optional condition when the policy should take effect"
    )
    column_mask: PolicyInfoColumnMask | None = Field(
        None,
        description="Options for column mask policies. Valid only if `policy_type` is `POLICY_TYPE_COLUMN_MASK`. Required on create and optional on update. When specified on update, the new options will replace the existing options as a whole",
    )
    deny: PolicyInfoDeny | None = Field(
        None,
        description="Options for deny policies. Valid only if `policy_type` is `POLICY_TYPE_DENY`. Required on create and optional on update. When specified on update, the new options will replace the existing options as a whole",
    )
    grant: PolicyInfoGrant | None = Field(
        None,
        description="Options for grant policies. Valid only if `policy_type` is `POLICY_TYPE_GRANT`. Required on create and optional on update. When specified on update, the new options will replace the existing options as a whole",
    )
    match_columns: list[PolicyInfoMatchColumns] | None = Field(
        None,
        description="Optional list of condition expressions used to match table columns. Only valid when `for_securable_type` is `TABLE`. When specified, the policy only applies to tables whose columns satisfy all match conditions",
    )
    provider_config: PolicyInfoProviderConfig | None = Field(
        None,
        description="Configure the provider for management through account provider",
    )
    row_filter: PolicyInfoRowFilter | None = Field(
        None,
        description="Options for row filter policies. Valid only if `policy_type` is `POLICY_TYPE_ROW_FILTER`. Required on create and optional on update. When specified on update, the new options will replace the existing options as a whole",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_policy_info"


__all__ = [
    "PolicyInfoBase",
    "PolicyInfoColumnMask",
    "PolicyInfoColumnMaskUsing",
    "PolicyInfoColumnMaskUsingFunctionArgExpression",
    "PolicyInfoColumnMaskUsingFunctionArgExpressionTagIntrospection",
    "PolicyInfoColumnMaskUsingFunctionArgExpressionTagIntrospectionColumnTagValue",
    "PolicyInfoColumnMaskUsingFunctionArgExpressionTagIntrospectionTagValue",
    "PolicyInfoDeny",
    "PolicyInfoGrant",
    "PolicyInfoMatchColumns",
    "PolicyInfoProviderConfig",
    "PolicyInfoRowFilter",
    "PolicyInfoRowFilterUsing",
    "PolicyInfoRowFilterUsingFunctionArgExpression",
    "PolicyInfoRowFilterUsingFunctionArgExpressionTagIntrospection",
    "PolicyInfoRowFilterUsingFunctionArgExpressionTagIntrospectionColumnTagValue",
    "PolicyInfoRowFilterUsingFunctionArgExpressionTagIntrospectionTagValue",
]
