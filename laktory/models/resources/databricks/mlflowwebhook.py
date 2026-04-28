from pydantic import ConfigDict

from laktory.models.resources.databricks.mlflowwebhook_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mlflowwebhook_base import MlflowWebhookBase


class MLflowWebhook(MlflowWebhookBase):
    """
    MLflow Webhook

    Examples
    --------
    ```py
    import io

    from laktory import models

    webhook_yaml = '''
    events:
    - TRANSITION_REQUEST_CREATED
    description: Databricks Job webhook trigger
    status: ACTIVE
    job_spec:
      job_id: some_id
      workspace_url: https://adb-1234567890.azuredatabricks.net
      access_token: some_token
    '''
    webhook = models.resources.databricks.MLflowWebhook.model_validate_yaml(
        io.StringIO(webhook_yaml)
    )
    ```

    References
    ----------

    * [MLflow Webhook](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mlflow_webhook)
    """

    model_config = ConfigDict(protected_namespaces=())

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        key = ""
        if self.model_name:
            key += self.model_name + "-"
        for e in self.events:
            key += e + "-"

        return key

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    # @property
    # def terraform_renames(self) -> dict[str, str]:

    # @property
    # def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
