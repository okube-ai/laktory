from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MwsNetworks

network = MwsNetworks(
    account_id="00000000-0000-0000-0000-000000000000",
    network_name="my-network",
    vpc_id="vpc-00000000",
    subnet_ids=["subnet-00000000", "subnet-00000001"],
    security_group_ids=["sg-00000000"],
)


def test_mwsnetworks():
    assert network.account_id == "00000000-0000-0000-0000-000000000000"
    assert network.network_name == "my-network"
    assert network.vpc_id == "vpc-00000000"
    assert network.subnet_ids == ["subnet-00000000", "subnet-00000001"]
    assert network.security_group_ids == ["sg-00000000"]
    assert network.resource_key == "my-network"
    assert network.terraform_resource_type == "databricks_mws_networks"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(network)
