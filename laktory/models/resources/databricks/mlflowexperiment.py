from pydantic import Field

from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.mlflowexperiment_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mlflowexperiment_base import (
    MlflowExperimentBase,
)
from laktory.models.resources.databricks.permissions import Permissions


class MLflowExperimentLookup(ResourceLookup):
    name: str = Field(description="Name (path) of the MLflow experiment")


class MLflowExperiment(MlflowExperimentBase):
    """
    MLflow Experiment

    Examples
    --------
    ```py
    import io

    from laktory import models

    exp_yaml = '''
    name: /.laktory/Sample
    artifact_location: dbfs:/tmp/my-experiment
    description: My MLflow experiment description
    access_controls:
    - group_name: account users
      permission_level: CAN_MANAGE
    '''
    exp = models.resources.databricks.MLflowExperiment.model_validate_yaml(
        io.StringIO(exp_yaml)
    )
    ```

    References
    ----------

    * [MLflow Experiment](https://mlflow.org/docs/latest/tracking.html)
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    lookup_existing: MLflowExperimentLookup = Field(
        None,
        exclude=True,
        description="Import a pre-existing MLflow Experiment by `name` instead of creating it. The experiment becomes available for cross-referencing; its own field values are not written to the existing resource.",
    )

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
                    resource_options={"name": f"permissions-{self.resource_name}"},
                    access_controls=self.access_controls,
                    experiment_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["access_controls"]
