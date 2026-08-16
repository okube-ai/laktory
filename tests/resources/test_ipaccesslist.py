from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import IpAccessList

ip_access_list = IpAccessList(
    label="office",
    list_type="ALLOW",
    ip_addresses=["1.2.3.4/32"],
)


def test_ip_access_list():
    assert ip_access_list.label == "office"
    assert ip_access_list.list_type == "ALLOW"
    assert ip_access_list.ip_addresses == ["1.2.3.4/32"]


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(ip_access_list)
