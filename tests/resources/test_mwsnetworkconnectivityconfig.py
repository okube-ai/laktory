from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MwsNetworkConnectivityConfig

ncc = MwsNetworkConnectivityConfig(
    name="my-ncc",
    region="eastus",
    workspace_bindings=[{"workspace_id": 1234567890}],
    private_endpoint_rules=[
        {
            "resource_names": ["my-bucket"],
            "endpoint_service": "com.amazonaws.vpce.us-east-1.vpce-svc-00000000000000000",
        }
    ],
)


def test_mwsnetworkconnectivityconfig():
    assert ncc.name == "my-ncc"
    assert ncc.region == "eastus"
    assert ncc.terraform_resource_type == "databricks_mws_network_connectivity_config"
    assert ncc.terraform_excludes == ["workspace_bindings", "private_endpoint_rules"]


def test_additional_core_resources():
    resources = ncc.additional_core_resources
    assert len(resources) == 2

    binding = resources[0]
    assert binding.workspace_id == 1234567890
    assert (
        binding.network_connectivity_config_id
        == f"${{resources.{ncc.resource_name}.network_connectivity_config_id}}"
    )

    rule = resources[1]
    assert rule.resource_names == ["my-bucket"]
    assert (
        rule.network_connectivity_config_id
        == f"${{resources.{ncc.resource_name}.network_connectivity_config_id}}"
    )


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(ncc)
