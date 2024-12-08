from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class MlflowModelTag(BaseModel):
    """
    MLflow model Tags Specifications

    Attributes
    ----------
    key:

    value:

    """

    key: str
    value: str


class MLflowModel(BaseModel, PulumiResource, TerraformResource):
    """
    MLflow Model

    Attributes
    ----------
    access_controls:
        MLflow model access controls
    description:
        The description of the MLflow model.
    name:
        Name of MLflow model. Change of name triggers new resource.
    tags:
        Tags for the MLflow model.

    Examples
    --------
    ```py
    from laktory import models

    mlmodel = models.resources.databricks.MLflowModel(
        name="My MLflow Model",
        description="My MLflow model description",
        tags=[
            {"key": "key1", "value": "value1"},
            {"key": "key2", "value": "value2"},
        ],
        access_controls=[
            {
                "group_name": "account users",
                "permission_level": "CAN_MANAGE_PRODUCTION_VERSIONS",
            }
        ],
    )
    ```
    """

    access_controls: list[AccessControl] = []
    description: str = None
    name: str
    tags: list[MlflowModelTag] = None

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
                    registered_model_id=f"${{resources.{self.resource_name}.registered_model_id}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:MlflowModel"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "tags": "tags",
        }

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mlflow_model"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
