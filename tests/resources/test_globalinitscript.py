from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import GlobalInitScript

script = GlobalInitScript(
    name="my-init-script",
    content_base64="ZWNobyAiaGVsbG8gd29ybGQi",
    enabled=True,
)


def test_globalinitscript():
    assert script.name == "my-init-script"
    assert script.content_base64 == "ZWNobyAiaGVsbG8gd29ybGQi"
    assert script.enabled
    assert script.resource_key == "my-init-script"
    assert script.terraform_resource_type == "databricks_global_init_script"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(script)
