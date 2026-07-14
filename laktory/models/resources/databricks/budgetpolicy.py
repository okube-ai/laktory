from laktory.models.resources.databricks.budgetpolicy_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.budgetpolicy_base import BudgetPolicyBase


class BudgetPolicy(BudgetPolicyBase):
    """
    Databricks budget policy

    Examples
    --------
    ```py
    import io

    from laktory import models

    budget_policy_yaml = '''
    policy_name: my-budget-policy
    custom_tags:
      - key: mykey
        value: myvalue
    '''
    budget_policy = models.resources.databricks.BudgetPolicy.model_validate_yaml(
        io.StringIO(budget_policy_yaml)
    )
    ```

    References
    ----------

    * [Databricks Budget Policy](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/budget_policy)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
