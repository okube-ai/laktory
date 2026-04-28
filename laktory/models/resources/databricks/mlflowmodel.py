from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.mlflowmodel_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.mlflowmodel_base import MlflowModelBase
from laktory.models.resources.databricks.permissions import Permissions


class MlflowModelTag(BaseModel):
    key: str = Field(..., description="")
    value: str = Field(..., description="")


class MLflowModel(MlflowModelBase):
    """
    MLflow Model

    Examples
    --------
    ```py
    import io

    from laktory import models

    model_yaml = '''
    name: my-mlflow-model
    description: My MLflow model description
    tags:
    - key: key1
      value: value1
    - key: key2
      value: value2
    access_controls:
    - group_name: account users
      permission_level: CAN_MANAGE_PRODUCTION_VERSIONS
    '''
    mlmodel = models.resources.databricks.MLflowModel.model_validate_yaml(
        io.StringIO(model_yaml)
    )
    ```

    References
    ----------

    * [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)
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
                    registered_model_id=f"${{resources.{self.resource_name}.registered_model_id}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]
