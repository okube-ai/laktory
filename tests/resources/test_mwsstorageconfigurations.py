from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MwsStorageConfigurations

storage = MwsStorageConfigurations(
    account_id="00000000-0000-0000-0000-000000000000",
    bucket_name="my-root-bucket",
    storage_configuration_name="my-storage-configuration",
)


def test_mwsstorageconfigurations():
    assert storage.account_id == "00000000-0000-0000-0000-000000000000"
    assert storage.bucket_name == "my-root-bucket"
    assert storage.storage_configuration_name == "my-storage-configuration"
    assert storage.resource_key == "my-storage-configuration"
    assert storage.terraform_resource_type == "databricks_mws_storage_configurations"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(storage)
