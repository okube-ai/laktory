from laktory.models.resources.databricks.budget_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.budget_base import BudgetBase


class Budget(BudgetBase):
    """
    Databricks budget

    Examples
    --------
    ```py
    import io

    from laktory import models

    budget_yaml = '''
    display_name: databricks-workspace-budget
    alert_configurations:
      - time_period: MONTH
        trigger_type: CUMULATIVE_SPENDING_EXCEEDED
        quantity_type: LIST_PRICE_DOLLARS_USD
        quantity_threshold: "840"
        action_configurations:
          - action_type: EMAIL_NOTIFICATION
            target: abc@gmail.com
    '''
    budget = models.resources.databricks.Budget.model_validate_yaml(
        io.StringIO(budget_yaml)
    )
    ```

    References
    ----------

    * [Databricks Budget](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/budget)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
