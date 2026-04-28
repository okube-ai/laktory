from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MLflowExperiment
from laktory.models.resources.databricks.permissions import Permissions

mlexp = MLflowExperiment(
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


def test_mlflow_experiment():
    assert mlexp.name == "/.laktory/Sample"
    assert mlexp.description == "My MLflow experiment description"


def test_mlflow_experiment_additional_resources():
    assert len(mlexp.additional_core_resources) == 1
    assert isinstance(mlexp.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(mlexp)


if __name__ == "__main__":
    test_mlflow_experiment()
