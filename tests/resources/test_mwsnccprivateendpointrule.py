from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MwsNccPrivateEndpointRule

rule = MwsNccPrivateEndpointRule(
    network_connectivity_config_id="ncc-id",
    resource_names=["my-bucket"],
    endpoint_service="com.amazonaws.vpce.us-east-1.vpce-svc-00000000000000000",
)


def test_mwsnccprivateendpointrule():
    assert rule.network_connectivity_config_id == "ncc-id"
    assert rule.resource_names == ["my-bucket"]
    assert (
        rule.endpoint_service
        == "com.amazonaws.vpce.us-east-1.vpce-svc-00000000000000000"
    )
    assert (
        rule.resource_key
        == "ncc-id-com.amazonaws.vpce.us-east-1.vpce-svc-00000000000000000"
    )
    assert rule.terraform_resource_type == "databricks_mws_ncc_private_endpoint_rule"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(rule)
