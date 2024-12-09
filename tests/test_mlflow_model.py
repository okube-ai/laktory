from laktory.models.resources.databricks import MLflowModel

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


if __name__ == "__main__":
    test_mlflow_model()
