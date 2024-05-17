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
    assert catalog.full_name == "dev"


if __name__ == "__main__":
    test_model()
