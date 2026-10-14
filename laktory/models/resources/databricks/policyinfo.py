from typing import Literal

from pydantic import Field

from laktory.models.resources.databricks.policyinfo_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.policyinfo_base import PolicyInfoBase


class PolicyInfo(PolicyInfoBase):
    """
    Databricks attribute-based access control (ABAC) policy

    Row filter, column mask, grant or deny policy defined on a catalog, schema
    or table and applied to the securables it contains, based on governed
    tags and the user identity. Policies can also be declared directly on
    `Catalog`, `Schema` and `Table` with their `policies` field.

    Examples
    --------
    ```py
    import io

    from laktory import models

    policy_yaml = '''
    name: mask_pii
    on_securable_type: SCHEMA
    on_securable_fullname: dev.finance
    policy_type: POLICY_TYPE_COLUMN_MASK
    for_securable_type: TABLE
    to_principals:
    - account users
    except_principals:
    - role-finance-admins
    match_columns:
    - condition: hasTag('pii')
      alias: pii_col
    column_mask:
      function_name: dev.finance.mask_string
      on_column: pii_col
    '''
    policy = models.resources.databricks.PolicyInfo.model_validate_yaml(
        io.StringIO(policy_yaml)
    )
    ```

    References
    ----------

    * [Databricks ABAC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/)
    * [Terraform databricks_policy_info](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/policy_info)
    """

    # Required on create
    name: str = Field(..., description="Name of the policy")

    # Narrow the types from base's plain str
    on_securable_type: Literal["CATALOG", "SCHEMA", "TABLE"] | None = Field(
        None,
        description="Type of the securable on which the policy is defined. Set automatically when declared in the policies of a catalog, schema or table.",
    )
    policy_type: Literal[
        "POLICY_TYPE_COLUMN_MASK",
        "POLICY_TYPE_DENY",
        "POLICY_TYPE_GRANT",
        "POLICY_TYPE_ROW_FILTER",
    ] = Field(..., description="Type of the policy")

    # Override description
    on_securable_fullname: str | None = Field(
        None,
        description="Full name of the securable on which the policy is defined. Set automatically when declared in the policies of a catalog, schema or table.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        if self.on_securable_fullname:
            return f"{self.on_securable_fullname}-{self.name}"
        return self.name
