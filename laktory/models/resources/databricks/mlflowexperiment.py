from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class MLflowExperiment(BaseModel, PulumiResource, TerraformResource):
    """
    MLflow Experiment

    Attributes
    ----------
    access_controls:
        MLflow Experiment access controls
    artifact_location:
        Path to dbfs:/ or s3:// artifact location of the MLflow experiment.
    creation_time:
    description:
        The description of the MLflow experiment.
    experiment_id:
    last_update_time:
    lifecycle_stage:
    name:
        Name of MLflow experiment. It must be an absolute path within the
        Databricks workspace, e.g. `/Users/<some-username>/my-experiment`.
        For more information about changes to experiment naming conventions,
        see [mlflow docs](https://docs.databricks.com/applications/mlflow/experiments.html#experiment-migration).


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

    access_controls: list[AccessControl] = []
    artifact_location: str = None
    creation_time: int = None
    description: str = None
    experiment_id: str = None
    last_update_time: int = None
    lifecycle_stage: str = None
    name: str

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.name.replace("/", "_").replace("\\", "_").replace(" ", "_")

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
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
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MlflowExperiment"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mlflow_experiment"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
