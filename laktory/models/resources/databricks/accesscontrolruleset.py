from laktory.models.resources.databricks.accesscontrolruleset_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.accesscontrolruleset_base import (
    AccessControlRuleSetBase,
)


class AccessControlRuleSet(AccessControlRuleSetBase):
    """
    Databricks Access Control Rule Set

    Examples
    --------
    ```py
    import io

    from laktory import models

    ruleset_yaml = '''
    name: accounts/acct-id/groups/group-id
    grant_rules:
    - role: roles/servicePrincipal.user
      principals:
      - users/user1@okube.ai
      - serviceAccounts/neptune@acct-id.iam.gserviceaccount.com
    '''
    ruleset = models.resources.databricks.AccessControlRuleSet.model_validate_yaml(
        io.StringIO(ruleset_yaml)
    )
    ```

    References
    ----------

    * [Databricks Access Control Rule Set](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/access_control_rule_set)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
