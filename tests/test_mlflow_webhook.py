from laktory.models.resources.databricks import MLflowWebhook

mlwebhook = MLflowWebhook(
    events=["TRANSITION_REQUEST_CREATED"],
    description="Databricks Job webhook trigger",
    status="ACTIVE",
    job_spec={
        "job_id": "some_id",
        "workspace_url": "some_url",
        "access_token": "some_token",
    },
)


def test_mlflow_webhook():

    assert mlwebhook.events == ["TRANSITION_REQUEST_CREATED"]
    assert mlwebhook.status == "ACTIVE"


if __name__ == "__main__":
    test_mlflow_webhook()
