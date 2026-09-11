# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_mws_workspaces
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class MwsWorkspacesCloudResourceContainerGcp(BaseModel):
    project_id: str = Field(
        ...,
        description="The Google Cloud project ID, which the workspace uses to instantiate cloud resources for your workspace",
    )


class MwsWorkspacesCloudResourceContainer(BaseModel):
    gcp: MwsWorkspacesCloudResourceContainerGcp | None = Field(
        None, description="A block that consists of the following field:"
    )


class MwsWorkspacesExternalCustomerInfo(BaseModel):
    authoritative_user_email: str = Field(...)
    authoritative_user_full_name: str = Field(...)
    customer_name: str = Field(...)


class MwsWorkspacesGcpManagedNetworkConfig(BaseModel):
    gke_cluster_pod_ip_range: str | None = Field(None)
    gke_cluster_service_ip_range: str | None = Field(None)
    subnet_cidr: str = Field(...)


class MwsWorkspacesGkeConfig(BaseModel):
    connectivity_type: str | None = Field(None)
    master_ip_range: str | None = Field(None)


class MwsWorkspacesTimeouts(BaseModel):
    create: str | None = Field(None)
    read: str | None = Field(None)
    update_: str | None = Field(
        None,
        serialization_alias="update",
        validation_alias=AliasChoices("update", "update_"),
    )


class MwsWorkspacesToken(BaseModel):
    comment: str | None = Field(
        None,
        description="Comment, that will appear in 'User Settings / Access Tokens' page on Workspace UI. By default it's 'Terraform PAT'",
    )
    lifetime_seconds: int | None = Field(
        None, description="Token expiry lifetime. By default its 2592000 (30 days)"
    )
    token_id: str | None = Field(None)
    token_value: str | None = Field(None)


class MwsWorkspacesBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_mws_workspaces`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    account_id: str = Field(
        ...,
        description="Account Id that could be found in the top right corner of [Accounts Console](https://accounts.cloud.databricks.com/)",
    )
    workspace_name: str = Field(
        ..., description="name of the workspace, will appear on UI"
    )
    aws_region: str | None = Field(None, description="(AWS only) region of VPC")
    cloud: str | None = Field(None)
    compute_mode: str | None = Field(
        None,
        description="- The compute mode for the workspace. When unset, a classic workspace is created, and both `credentials_id` and `storage_configuration_id` must be specified. When set to `SERVERLESS`, the resulting workspace is a serverless workspace, and `credentials_id` and `storage_configuration_id` must not be set. The only allowed value for this is `SERVERLESS`. Changing this field requires recreation of the workspace",
    )
    creation_time: int | None = Field(
        None, description="(Integer) time when workspace was created"
    )
    credentials_id: str | None = Field(
        None,
        description="(AWS only, Optional) `credentials_id` from credentials. This must not be specified when `compute_mode` is set to `SERVERLESS`",
    )
    custom_tags: dict[str, str] | None = Field(
        None, description="(Map) Custom Tags (if present) added to workspace"
    )
    customer_managed_key_id: str | None = Field(None)
    deployment_name: str | None = Field(
        None,
        description="part of URL as in `https://<prefix>-<deployment-name>.cloud.databricks.com`. Deployment name cannot be used until a deployment name prefix is defined. Please contact your Databricks representative. Once a new deployment prefix is added/updated, it only will affect the new workspaces created",
    )
    expected_workspace_status: str | None = Field(
        None,
        description="- The expected status of the workspace. When unset, it defaults to `RUNNING`. When set to `PROVISIONING`, workspace provisioning will pause and not enter `RUNNING` status. The only allowed values for this is `RUNNING` and `PROVISIONING`",
    )
    is_no_public_ip_enabled: bool | None = Field(None)
    location: str | None = Field(None, description="(GCP only) region of the subnet")
    managed_services_customer_managed_key_id: str | None = Field(
        None,
        description="`customer_managed_key_id` from customer managed keys with `use_cases` set to `MANAGED_SERVICES`. This is used to encrypt the workspace's notebook and secret data in the control plane",
    )
    network_connectivity_config_id: str | None = Field(None)
    network_id: str | None = Field(
        None, description="(Optional) `network_id` from networks"
    )
    pricing_tier: str | None = Field(
        None, description="- The pricing tier of the workspace"
    )
    private_access_settings_id: str | None = Field(
        None,
        description="Canonical unique identifier of databricks_mws_private_access_settings in Databricks Account",
    )
    storage_configuration_id: str | None = Field(
        None,
        description="(AWS only, Optional) `storage_configuration_id` from storage configuration. This must not be specified when `compute_mode` is set to `SERVERLESS`",
    )
    storage_customer_managed_key_id: str | None = Field(
        None,
        description="`customer_managed_key_id` from customer managed keys with `use_cases` set to `STORAGE`. This is used to encrypt the DBFS Storage & Cluster Volumes",
    )
    workspace_id: int | None = Field(None, description="(String) workspace id")
    workspace_status: str | None = Field(None, description="(String) workspace status")
    workspace_status_message: str | None = Field(
        None, description="(String) updates on workspace status"
    )
    workspace_url: str | None = Field(None, description="(String) URL of the workspace")
    cloud_resource_container: MwsWorkspacesCloudResourceContainer | None = Field(
        None,
        description="(GCP only) A block that specifies GCP workspace configurations, consisting of following blocks:",
    )
    external_customer_info: MwsWorkspacesExternalCustomerInfo | None = Field(None)
    gcp_managed_network_config: MwsWorkspacesGcpManagedNetworkConfig | None = Field(
        None
    )
    gke_config: MwsWorkspacesGkeConfig | None = Field(None)
    timeouts: MwsWorkspacesTimeouts | None = Field(None)
    token: MwsWorkspacesToken | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_mws_workspaces"


__all__ = [
    "MwsWorkspacesBase",
    "MwsWorkspacesCloudResourceContainer",
    "MwsWorkspacesCloudResourceContainerGcp",
    "MwsWorkspacesExternalCustomerInfo",
    "MwsWorkspacesGcpManagedNetworkConfig",
    "MwsWorkspacesGkeConfig",
    "MwsWorkspacesTimeouts",
    "MwsWorkspacesToken",
]
