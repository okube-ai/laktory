from laktory.models.resources.databricks import MLflowExperiment

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


if __name__ == "__main__":
    test_mlflow_experiment()
