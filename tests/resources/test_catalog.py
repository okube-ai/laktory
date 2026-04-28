from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Catalog

catalog = Catalog(
    name="dev",
    grants=[
        {"principal": "account users", "privileges": ["USE_CATALOG", "USE_SCHEMA"]}
    ],
    schemas=[
        {
            "name": "engineering",
            "grants": [{"principal": "domain-engineering", "privileges": ["SELECT"]}],
        },
        {
            "name": "sources",
            "volumes": [
                {
                    "name": "landing",
                    "volume_type": "EXTERNAL",
                    "grants": [
                        {
                            "principal": "account users",
                            "privileges": ["READ_VOLUME"],
                        },
                        {
                            "principal": "role-metastore-admins",
                            "privileges": ["WRITE_VOLUME"],
                        },
                    ],
                },
            ],
        },
    ],
)


def test_model():
    assert catalog.name == "dev"
    assert catalog.full_name == "dev"


def test_catalog_additional_resources():
    resources = catalog.additional_core_resources
    types_found = {type(r).__name__ for r in resources}
    assert "Grants" in types_found
    assert "Schema" in types_found


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(catalog)
