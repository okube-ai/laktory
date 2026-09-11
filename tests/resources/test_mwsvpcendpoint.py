from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import MwsVpcEndpoint

endpoint = MwsVpcEndpoint(
    vpc_endpoint_name="my-vpc-endpoint",
    aws_vpc_endpoint_id="vpce-00000000",
    region="us-east-1",
)


def test_mwsvpcendpoint():
    assert endpoint.vpc_endpoint_name == "my-vpc-endpoint"
    assert endpoint.aws_vpc_endpoint_id == "vpce-00000000"
    assert endpoint.region == "us-east-1"
    assert endpoint.resource_key == "my-vpc-endpoint"
    assert endpoint.terraform_resource_type == "databricks_mws_vpc_endpoint"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(endpoint)
