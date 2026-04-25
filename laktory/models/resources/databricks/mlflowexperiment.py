from typing import Union

from pydantic import Field

from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.mlflowexperiment_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mlflowexperiment_base import (
    MlflowExperimentBase,
)
from laktory.models.resources.databricks.permissions import Permissions


class MLflowExperiment(MlflowExperimentBase):
    """
    MLflow Experiment

    Examples
    --------
    ```py
    from laktory import models

    exp = models.resources.databricks.MLflowExperiment(
        name="/.laktory/Sample",
        artifact_location="dbfs:/tmp/my-experiment",
        description="My MLflow experiment description",
        access_controls=[
            {
                "group_name": "account users",
                "permission_level": "CAN_MANAGE",
            }
        ],
    )
    ```
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.name

    @property
    def additional_core_resources(self) -> list:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    experiment_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]
