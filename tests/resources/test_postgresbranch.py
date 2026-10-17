from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import PostgresBranch
from laktory.models.resources.databricks import PostgresCatalog
from laktory.models.resources.databricks import PostgresEndpoint
from laktory.models.resources.databricks import PostgresRole
from laktory.models.resources.databricks import PostgresSnapshotSchedule
from laktory.models.resources.databricks import PostgresSyncedTable


def get_branch():
    return PostgresBranch.model_validate(
        {
            "parent": "projects/laktory-app",
            "branch_id": "dev",
            "spec": {"no_expiry": True},
            "endpoints": [
                {
                    "endpoint_id": "primary",
                    "replace_existing": True,
                    "spec": {
                        "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
                        "autoscaling_limit_min_cu": 0.5,
                        "autoscaling_limit_max_cu": 2.0,
                    },
                }
            ],
            "roles": [
                {
                    "role_id": "role-engineers",
                    "spec": {
                        "identity_type": "GROUP",
                        "postgres_role": "role-engineers",
                    },
                }
            ],
            "catalogs": [
                {
                    "catalog_id": "laktory_app_pg",
                    "spec": {"postgres_database": "app_db"},
                }
            ],
            "synced_tables": [
                {
                    "synced_table_id": "laktory_app_pg.public.stock_prices",
                    "spec": {
                        "postgres_database": "app_db",
                        "source_table_full_name": "laktory.unit_tests.stock_prices",
                        "primary_key_columns": ["symbol"],
                        "scheduling_policy": "SNAPSHOT",
                        "new_pipeline_spec": {
                            "storage_catalog": "laktory",
                            "storage_schema": "unit_tests",
                        },
                    },
                }
            ],
            "snapshot_schedule": {
                "schedule": [{"daily_schedule": {"hour": 3}, "retention": "168h0m0s"}]
            },
        }
    )


def test_postgresbranch():
    branch = get_branch()
    assert branch.resource_name == "postgres-branch-laktory-app-dev"
    props = branch.terraform_properties
    for k in ["endpoints", "roles", "catalogs", "synced_tables", "snapshot_schedule"]:
        assert k not in props

    branch_name = "projects/laktory-app/branches/dev"
    resources = {type(r): r for r in branch.additional_core_resources}
    endpoint = resources[PostgresEndpoint]
    assert endpoint.parent == branch_name
    assert endpoint.resource_name == "postgres-endpoint-laktory-app-dev-primary"
    assert resources[PostgresRole].parent == branch_name
    assert resources[PostgresSnapshotSchedule].parent == branch_name
    assert resources[PostgresSnapshotSchedule].resource_name == (
        "postgres-snapshot-schedule-laktory-app-dev"
    )
    assert resources[PostgresCatalog].spec.branch == branch_name
    assert resources[PostgresCatalog].resource_name == "postgres-catalog-laktory_app_pg"
    synced_table = resources[PostgresSyncedTable]
    assert synced_table.spec.branch == branch_name
    assert synced_table.resource_name == (
        "postgres-synced-table-laktory_app_pg-public-stock_prices"
    )


def test_terraform_plan():
    skip_terraform_plan()
    branch = get_branch()
    plan_resource(branch.model_copy(update={"synced_tables": None, "catalogs": None}))
    for r in branch.additional_core_resources:
        plan_resource(r)
