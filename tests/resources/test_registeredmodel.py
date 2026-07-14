from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.grants import RegisteredModelGrant
from laktory.models.resources.databricks import RegisteredModel
from laktory.models.resources.databricks.grants import Grants

registered_model = RegisteredModel(
    name="my-model",
    catalog_name="main",
    schema_name="default",
    comment="Registered model for my project",
    grants=[{"principal": "account users", "privileges": ["EXECUTE"]}],
)


def test_registered_model():
    assert registered_model.name == "my-model"
    assert registered_model.catalog_name == "main"
    assert registered_model.schema_name == "default"
    assert registered_model.full_name == "main.default.my-model"
    assert registered_model.resource_name == "registered-model-main-default-my-model"
    assert registered_model.grants[0] == RegisteredModelGrant(
        principal="account users", privileges=["EXECUTE"]
    )
    assert len(registered_model.additional_core_resources) == 1
    for r in registered_model.additional_core_resources:
        assert isinstance(r, Grants)
        assert r.resource_name == "grants-registered-model-main-default-my-model"


def test_lookup_existing():
    rm = RegisteredModel(lookup_existing={"name": "dev.engineering.other-model"})
    assert rm.full_name == "dev.engineering.other-model"
    assert rm.resource_name == "registered-model-dev-engineering-other-model"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(registered_model)
