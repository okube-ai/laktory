from typing import Union

from laktory.models.resources.databricks.accesscontrolruleset_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.accesscontrolruleset_base import (
    AccessControlRuleSetBase,
)
from laktory.models.resources.pulumiresource import PulumiResource


class AccessControlRuleSet(AccessControlRuleSetBase, PulumiResource):
    """
    Databricks Access Control Rule Set

    References
    ----------
    * [pulumi](https://www.pulumi.com/registry/packages/databricks/api-docs/accesscontrolruleset/)
    * [terraform](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/access_control_rule_set)
    """

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
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
