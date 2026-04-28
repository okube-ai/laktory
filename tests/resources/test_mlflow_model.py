from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MLflowModel
from laktory.models.resources.databricks.permissions import Permissions

mlmodel = MLflowModel(
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


def test_mlflow_model():
    assert mlmodel.name == "My MLflow Model"
    assert mlmodel.description == "My MLflow model description"


def test_mlflow_model_additional_resources():
    assert len(mlmodel.additional_core_resources) == 1
    assert isinstance(mlmodel.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(mlmodel)
