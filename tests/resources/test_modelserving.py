from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import ModelServing
from laktory.models.resources.databricks.permissions import Permissions

model_serving = ModelServing(
    name="my-endpoint",
    config={
        "served_entities": [
            {
                "entity_name": "main.default.my_model",
                "entity_version": "1",
                "workload_size": "Small",
                "scale_to_zero_enabled": True,
            }
        ]
    },
    access_controls=[{"permission_level": "CAN_QUERY", "group_name": "account users"}],
)


def test_model_serving():
    assert model_serving.name == "my-endpoint"
    assert (
        model_serving.config.served_entities[0].entity_name == "main.default.my_model"
    )
    assert model_serving.config.served_entities[0].scale_to_zero_enabled is True


def test_model_serving_additional_resources():
    assert len(model_serving.additional_core_resources) == 1
    assert isinstance(model_serving.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(model_serving)
