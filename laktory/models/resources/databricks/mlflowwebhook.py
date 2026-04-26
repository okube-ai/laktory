from pydantic import ConfigDict

from laktory.models.resources.databricks.mlflowwebhook_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mlflowwebhook_base import MlflowWebhookBase


class MLflowWebhook(MlflowWebhookBase):
    """
    MLflow Model

    Examples
    --------
    ```py
    from laktory import models

    mlwebhook = models.resources.databricks.MLflowWebhook(
        events=["TRANSITION_REQUEST_CREATED"],
        description="Databricks Job webhook trigger",
        status="ACTIVE",
        job_spec={
            "job_id": "some_id",
            "workspace_url": "some_url",
            "access_token": "some_token",
        },
    )
    ```
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
