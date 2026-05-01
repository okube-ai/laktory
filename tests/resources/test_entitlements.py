from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Entitlements

entitlements = Entitlements(
    user_id="some_user_id",
    allow_cluster_create=True,
    allow_instance_pool_create=True,
    databricks_sql_access=True,
    workspace_access=True,
)


def test_entitlements():
    assert entitlements.user_id == "some_user_id"
    assert entitlements.allow_cluster_create is True
    assert entitlements.allow_instance_pool_create is True
    assert entitlements.databricks_sql_access is True
    assert entitlements.workspace_access is True
    assert entitlements.resource_key == "user-some_user_id"


def test_entitlements_group():
    e = Entitlements(group_id="some_group_id", allow_cluster_create=True)
    assert e.resource_key == "group-some_group_id"


def test_entitlements_spn():
    e = Entitlements(service_principal_id="some_spn_id", workspace_access=True)
    assert e.resource_key == "spn-some_spn_id"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(entitlements)
