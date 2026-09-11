from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MwsPrivateAccessSettings

settings = MwsPrivateAccessSettings(
    private_access_settings_name="my-private-access-settings",
    region="us-east-1",
    public_access_enabled=False,
)


def test_mwsprivateaccesssettings():
    assert settings.private_access_settings_name == "my-private-access-settings"
    assert settings.region == "us-east-1"
    assert settings.public_access_enabled is False
    assert settings.resource_key == "my-private-access-settings"
    assert settings.terraform_resource_type == "databricks_mws_private_access_settings"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(settings)
