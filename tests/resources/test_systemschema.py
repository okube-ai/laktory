from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import SystemSchema

system_schema = SystemSchema.model_validate({"schema": "access"})


def test_systemschema():
    assert system_schema.schema_ == "access"
    assert system_schema.resource_key == "access"
    assert system_schema.terraform_resource_type == "databricks_system_schema"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(system_schema)
