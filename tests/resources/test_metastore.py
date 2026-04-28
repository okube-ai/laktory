from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Metastore

metastore = Metastore(
    name="metastore-lakehouse",
    storage_root="abfss://metastore@o3stglakehousedev.dfs.core.windows.net/",
    region="eastus",
    force_destroy=True,
    grants_provider="${resources.provider-workspace-neptune}",
    workspace_assignments=[{"workspace_id": 0}],
    grants=[
        {
            "principal": "role-metastore-admins",
            "privileges": [
                "CREATE_CATALOG",
                "CREATE_CONNECTION",
                "CREATE_EXTERNAL_LOCATION",
                "CREATE_STORAGE_CREDENTIAL",
                "MANAGE_ALLOWLIST",
            ],
        }
    ],
)


def test_metastore():
    assert metastore.name == "metastore-lakehouse"
    assert metastore.region == "eastus"
    assert metastore.grants[0].principal == "role-metastore-admins"


def test_metastore_additional_resources():
    resources = metastore.additional_core_resources
    types_found = {type(r).__name__ for r in resources}
    assert "MetastoreAssignment" in types_found
    assert "Grants" in types_found


def test_terraform_plan():
    skip_terraform_plan()
    metastore_simple = Metastore(
        name="metastore-test",
        storage_root="abfss://metastore@storage.dfs.core.windows.net/",
        region="eastus",
    )
    plan_resource(metastore_simple)
