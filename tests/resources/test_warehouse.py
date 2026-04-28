import pytest

from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Warehouse
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.databricks.warehouse import WarehouseLookup

warehouse = Warehouse(
    name="default",
    cluster_size="2X-Small",
    auto_stop_mins=30,
    channel={"name": "CHANNEL_NAME_PREVIEW"},
    enable_photon=True,
    enable_serverless_compute=True,
    access_controls=[
        {"group_name": "account warehouses", "permission_level": "CAN_USE"}
    ],
)


def test_warehouse():
    assert warehouse.name == "default"
    assert warehouse.cluster_size == "2X-Small"


def test_both_warehouse_id_and_warehouse_name():
    with pytest.raises(ValueError):
        WarehouseLookup(id=123, name="test_warehouse")


def test_neither_warehouse_id_and_warehouse_name():
    with pytest.raises(ValueError):
        WarehouseLookup()


def test_warehouse_additional_resources():
    assert len(warehouse.additional_core_resources) == 1
    assert isinstance(warehouse.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(warehouse)


if __name__ == "__main__":
    test_warehouse()
    test_both_warehouse_id_and_warehouse_name()
    test_neither_warehouse_id_and_warehouse_name()
