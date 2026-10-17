import json

from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import ShareProvider

provider = ShareProvider(
    name="laktory-partner",
    authentication_type="TOKEN",
    recipient_profile_str=json.dumps(
        {
            "shareCredentialsVersion": 1,
            "bearerToken": "dapi-token",
            "endpoint": "https://sharing.delta.io/delta-sharing/",
        }
    ),
)


def test_shareprovider():
    assert provider.resource_name == "share-provider-laktory-partner"
    assert provider.terraform_resource_type == "databricks_provider"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(provider)
