"""
Shared constants and naming helpers used across the build_resources scripts.
"""

from __future__ import annotations

# Maps a resource_key to an alternate naming key used for class/file naming.
# The terraform_resource_type property still returns the original resource_key.
RESOURCE_NAME_OVERRIDES: dict[str, str] = {
    "databricks_sql_table": "databricks_table",
}

# Default generation targets
DEFAULT_TARGETS: list[str] = [
    "databricks_access_control_rule_set",
    "databricks_alert",
    "databricks_app",
    "databricks_catalog",
    "databricks_cluster",
    "databricks_cluster_policy",
    "databricks_current_user",
    "databricks_dashboard",
    "databricks_dbfs_file",
    "databricks_directory",
    "databricks_entitlements",
    "databricks_external_location",
    "databricks_grant",
    "databricks_grants",
    "databricks_group",
    "databricks_group_member",
    "databricks_job",
    "databricks_metastore",
    "databricks_metastore_assignment",
    "databricks_metastore_data_access",
    "databricks_mlflow_experiment",
    "databricks_mlflow_model",
    "databricks_mlflow_webhook",
    "databricks_mws_ncc_binding",
    "databricks_mws_network_connectivity_config",
    "databricks_mws_permission_assignment",
    "databricks_notebook",
    "databricks_notification_destination",
    "databricks_obo_token",
    "databricks_permissions",
    "databricks_pipeline",
    "databricks_quality_monitor",
    "databricks_query",
    "databricks_recipient",
    "databricks_repo",
    "databricks_schema",
    "databricks_secret",
    "databricks_secret_acl",
    "databricks_secret_scope",
    "databricks_service_principal",
    "databricks_service_principal_role",
    "databricks_share",
    "databricks_sql_endpoint",  # Warehouse
    "databricks_sql_table",
    "databricks_storage_credential",
    "databricks_user",
    "databricks_user_role",
    "databricks_vector_search_endpoint",
    "databricks_vector_search_index",
    "databricks_volume",
    "databricks_workspace_binding",
    "databricks_workspace_file",
]


def resource_to_class_name(resource_key: str) -> str:
    """Convert a Terraform resource key to a Python class name.

    "databricks_catalog" -> "Catalog"
    """
    name = resource_key.removeprefix("databricks_")
    return "".join(part.capitalize() for part in name.split("_"))


def base_file_stem(naming_key: str) -> str:
    """Return the stem of the generated *_base.py file for a naming key.

    "databricks_sql_endpoint" -> "sqlendpoint"
    """
    return naming_key.removeprefix("databricks_").replace("_", "")
