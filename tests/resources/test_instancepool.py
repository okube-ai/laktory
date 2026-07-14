from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import InstancePool
from laktory.models.resources.databricks.permissions import Permissions

instance_pool = InstancePool(
    instance_pool_name="default",
    node_type_id="Standard_DS3_v2",
    min_idle_instances=0,
    idle_instance_autotermination_minutes=10,
    access_controls=[
        {"permission_level": "CAN_ATTACH_TO", "group_name": "account users"}
    ],
)


def test_instance_pool():
    assert instance_pool.instance_pool_name == "default"
    assert instance_pool.node_type_id == "Standard_DS3_v2"


def test_instance_pool_additional_resources():
    assert len(instance_pool.additional_core_resources) == 1
    assert isinstance(instance_pool.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(instance_pool)
