from laktory.models.resources.databricks import Metastore


metastore = Metastore(
    name="metastore-lakehouse",
    cloud="azure",
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
    data_accesses=[
        {
            "name": "lakehouse-dev",
            "azure_managed_identity": {"access_connector_id": "test"},
            "force_destroy": True,
            "is_default": False,
            "grants": [
                {
                    "principal": "role-metastore-admins",
                    "privileges": [
                        "CREATE_EXTERNAL_LOCATION",
                    ],
                }
            ],
        }
    ],
)


def test_metastore():
    data = metastore.model_dump()
    print(data)

    assert data == {
        "cloud": "azure",
        "created_at": None,
        "created_by": None,
        "data_accesses": [
            {
                "aws_iam_role": None,
                "azure_managed_identity": {
                    "access_connector_id": "test",
                    "credential_id": None,
                    "managed_identity_id": None,
                },
                "azure_service_principal": None,
                "comment": None,
                "databricks_gcp_service_account": None,
                "force_destroy": True,
                "force_update": None,
                "gcp_service_account_key": None,
                "grants": [
                    {
                        "principal": "role-metastore-admins",
                        "privileges": ["CREATE_EXTERNAL_LOCATION"],
                    }
                ],
                "is_default": False,
                "metastore_id": None,
                "name": "lakehouse-dev",
                "owner": None,
                "read_only": None,
                "skip_validation": None,
            }
        ],
        "default_data_access_config_id": None,
        "delta_sharing_organization_name": None,
        "delta_sharing_recipient_token_lifetime_in_seconds": None,
        "delta_sharing_scope": None,
        "force_destroy": True,
        "global_metastore_id": None,
        "grants": [
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
        "metastore_id": None,
        "name": "metastore-lakehouse",
        "owner": None,
        "region": "eastus",
        "storage_root": "abfss://metastore@o3stglakehousedev.dfs.core.windows.net/",
        "storage_root_credential_id": None,
        "updated_at": None,
        "updated_by": None,
        "grants_provider": "${resources.provider-workspace-neptune}",
        "workspace_assignments": [
            {"default_catalog_name": None, "metastore_id": None, "workspace_id": 0}
        ],
    }


if __name__ == "__main__":
    test_metastore()
