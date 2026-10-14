from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Credential

credential = Credential(
    name="laktory-service-credential",
    purpose="SERVICE",
    azure_managed_identity={
        "access_connector_id": "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/providers/Microsoft.Databricks/accessConnectors/ac"
    },
    comment="laktory unit test",
)


def test_credential():
    assert credential.resource_key == "laktory-service-credential"
    assert credential.azure_managed_identity.access_connector_id.endswith("/ac")
    assert credential.terraform_resource_type == "databricks_credential"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(credential)
