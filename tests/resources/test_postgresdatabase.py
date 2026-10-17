from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import PostgresCdfConfig
from laktory.models.resources.databricks import PostgresDataApi
from laktory.models.resources.databricks import PostgresDatabase


def get_database():
    return PostgresDatabase.model_validate(
        {
            "parent": "projects/laktory-app/branches/dev",
            "database_id": "app-db",
            "spec": {
                "postgres_database": "app_db",
                "role": "projects/laktory-app/branches/dev/roles/app-owner",
            },
            "data_api": {"spec": {"db_schemas": ["public"]}},
            "cdf_configs": [
                {"postgres_schema": "public", "catalog": "laktory", "schema": "app_rep"}
            ],
        }
    )


def test_postgresdatabase():
    database = get_database()
    assert database.resource_name == ("postgres-database-laktory-app-dev-app-db")
    props = database.terraform_properties
    assert "data_api" not in props
    assert "cdf_configs" not in props

    database_name = "projects/laktory-app/branches/dev/databases/app-db"
    api, cdf = database.additional_core_resources
    assert isinstance(api, PostgresDataApi)
    assert api.parent == database_name
    assert api.resource_name == ("postgres-data-api-laktory-app-dev-app-db")
    assert isinstance(cdf, PostgresCdfConfig)
    assert cdf.parent == database_name
    assert cdf.schema_ == "app_rep"
    assert cdf.terraform_properties["schema"] == "app_rep"
    assert cdf.resource_name == ("postgres-cdf-config-laktory-app-dev-app-db-public")


def test_terraform_plan():
    skip_terraform_plan()
    database = get_database()
    plan_resource(database.model_copy(update={"data_api": None, "cdf_configs": None}))
    for r in database.additional_core_resources:
        plan_resource(r)
