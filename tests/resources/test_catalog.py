from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Catalog
from laktory.models.resources.databricks import EntityTagAssignment
from laktory.models.resources.databricks import PolicyInfo

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


def test_catalog_tags_and_policies():
    cat = Catalog(
        name="dev",
        tags={"environment": "dev", "certified": None},
        policies=[
            {
                "name": "filter_region",
                "policy_type": "POLICY_TYPE_ROW_FILTER",
                "for_securable_type": "TABLE",
                "to_principals": ["account users"],
                "row_filter": {"function_name": "dev.filters.region"},
            }
        ],
    )
    resources = cat.additional_core_resources
    tags = [r for r in resources if isinstance(r, EntityTagAssignment)]
    assert [(t.entity_type, t.entity_name, t.tag_key, t.tag_value) for t in tags] == [
        ("catalogs", "${resources.catalog-dev.id}", "environment", "dev"),
        ("catalogs", "${resources.catalog-dev.id}", "certified", None),
    ]
    (policy,) = [r for r in resources if isinstance(r, PolicyInfo)]
    assert policy.on_securable_type == "CATALOG"
    assert policy.on_securable_fullname == "${resources.catalog-dev.id}"
    assert policy.resource_name == "policy-info-catalog-dev-filter_region"
    assert "tags" not in cat.terraform_properties
    assert "policies" not in cat.terraform_properties


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(catalog)
