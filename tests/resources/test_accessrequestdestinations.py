from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import AccessRequestDestinations

destinations = AccessRequestDestinations.model_validate(
    {
        "securable": {"type": "SCHEMA", "full_name": "laktory.unit_tests"},
        "destinations": [
            {"destination_type": "EMAIL", "destination_id": "owners@okube.ai"}
        ],
    }
)


def test_accessrequestdestinations():
    assert destinations.destinations[0].destination_type == "EMAIL"
    assert destinations.resource_name == (
        "access-request-destinations-SCHEMA-laktory-unit_tests"
    )
    assert (
        destinations.terraform_resource_type
        == "databricks_rfa_access_request_destinations"
    )


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(destinations)
