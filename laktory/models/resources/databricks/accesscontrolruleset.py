from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class AccessControlRuleSetGrantRule(BaseModel):
    """Access Control Rule Set Grant Rule"""

    role: str = Field(..., description="Role to be granted.")
    principals: list[str] = Field(
        None,
        description="a list of principals who are granted a role.",
    )


class AccessControlRuleSet(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Access Control Rule Set

    References
    ----------
    * [pulumi](https://www.pulumi.com/registry/packages/databricks/api-docs/accesscontrolruleset/)
    * [terraform](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/access_control_rule_set)
    """

    grant_rules: list[AccessControlRuleSetGrantRule] = Field(
        None,
        description="The access control rules to be granted by this rule set, consisting of a set of principals and roles to be granted to them.",
    )
    name: str = Field(
        ...,
        description="Unique identifier of a rule set. The name determines the resource to which the rule set applies. Changing the name recreates the resource.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:AccessControlRuleSet"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "grant_rules": "grant_rules",
        }

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_access_control_rule_set"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
