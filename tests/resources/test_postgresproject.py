from laktory import models
from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import PostgresBranch
from laktory.models.resources.databricks import PostgresProject


def get_project():
    return PostgresProject.model_validate(
        {
            "project_id": "laktory-app",
            "spec": {"pg_version": 17, "display_name": "Laktory App"},
            "branches": [
                {"branch_id": "production", "replace_existing": True},
                {"branch_id": "dev", "spec": {"no_expiry": True}},
            ],
        }
    )


def test_postgresproject():
    project = get_project()
    assert project.resource_name == "postgres-project-laktory-app"
    assert project.terraform_resource_type == "databricks_postgres_project"
    assert "branches" not in project.terraform_properties

    branches = project.additional_core_resources
    assert all(isinstance(b, PostgresBranch) for b in branches)
    assert [b.resource_name for b in branches] == [
        "postgres-branch-projects-laktory-app-branches-production",
        "postgres-branch-projects-laktory-app-branches-dev",
    ]
    assert {b.parent for b in branches} == {"projects/laktory-app"}

    # Idempotent, and explicit names are kept
    assert [b.resource_name for b in project.additional_core_resources] == [
        "postgres-branch-projects-laktory-app-branches-production",
        "postgres-branch-projects-laktory-app-branches-dev",
    ]
    project = PostgresProject.model_validate(
        {
            "project_id": "laktory-app",
            "branches": [{"branch_id": "dev", "resource_options": {"name": "pg-dev"}}],
        }
    )
    assert project.additional_core_resources[0].resource_name == "pg-dev"


def test_stack_nested_names():
    # Nested resources are named from the ids path, also through the stack
    # variables injection that builds nested resources before their parents
    stack = models.Stack(
        name="test",
        organization="test",
        resources={
            "databricks_postgresprojects": {
                "postgres-project-app": {
                    "project_id": "app",
                    "branches": [
                        {
                            "branch_id": "production",
                            "endpoints": [{"endpoint_id": "primary"}],
                            "databases": [
                                {
                                    "database_id": "app-db",
                                    "data_api": {"spec": {"db_schemas": ["public"]}},
                                }
                            ],
                            "catalogs": [
                                {
                                    "catalog_id": "app_pg",
                                    "spec": {"postgres_database": "app_db"},
                                }
                            ],
                        }
                    ],
                }
            },
            "providers": {"databricks": {"host": "https://adb.azuredatabricks.net"}},
        },
        environments={"dev": {}},
    )
    resources = stack.to_terraform(env_name="dev").model_dump()["resource"]
    names = {t: list(rs) for t, rs in resources.items()}
    assert names["databricks_postgres_branch"] == [
        "postgres-branch-projects-app-branches-production"
    ]
    assert names["databricks_postgres_endpoint"] == [
        "postgres-endpoint-projects-app-branches-production-endpoints-primary"
    ]
    assert names["databricks_postgres_data_api"] == [
        "postgres-data-api-projects-app-branches-production-databases-app-db"
    ]
    endpoint = resources["databricks_postgres_endpoint"][
        "postgres-endpoint-projects-app-branches-production-endpoints-primary"
    ]
    assert endpoint["parent"] == "projects/app/branches/production"
    assert endpoint["depends_on"] == [
        "databricks_postgres_branch.postgres-branch-projects-app-branches-production"
    ]
    catalog = resources["databricks_postgres_catalog"]["postgres-catalog-app_pg"]
    assert catalog["spec"]["branch"] == "projects/app/branches/production"


def test_forked_branch_created_after_source():
    project = PostgresProject.model_validate(
        {
            "project_id": "app",
            "branches": [
                {
                    "branch_id": "feature-x",
                    "spec": {"source_branch": "projects/app/branches/staging"},
                },
                {"branch_id": "staging"},
                {
                    "branch_id": "dev",
                    "spec": {"source_branch": "projects/app/branches/production"},
                },
            ],
        }
    )
    feature, staging, dev = project.additional_core_resources
    assert feature.resource_options.depends_on == [
        f"${{resources.{staging.resource_name}}}"
    ]
    # Source branch not declared in the project (e.g. implicit production)
    assert staging.resource_options.depends_on == []
    assert dev.resource_options.depends_on == []


def test_names_unique_across_projects():
    # Ids joined with `-` must not make different paths share a name
    branches = [
        PostgresProject(project_id=p, branches=[{"branch_id": b}]).branches[0]
        for p, b in [("a-b", "c"), ("a", "b-c")]
    ]
    assert branches[0].resource_name != branches[1].resource_name


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(get_project())
