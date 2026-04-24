# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_external_location
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class ExternalLocationEffectiveFileEventQueueManagedAqs(BaseModel):
    managed_resource_id: str | None = Field(None)
    queue_url: str | None = Field(None)
    resource_group: str | None = Field(None)
    subscription_id: str | None = Field(None)


class ExternalLocationEffectiveFileEventQueueManagedPubsub(BaseModel):
    managed_resource_id: str | None = Field(None)
    subscription_name: str | None = Field(None)


class ExternalLocationEffectiveFileEventQueueManagedSqs(BaseModel):
    managed_resource_id: str | None = Field(None)
    queue_url: str | None = Field(None)


class ExternalLocationEffectiveFileEventQueueProvidedAqs(BaseModel):
    managed_resource_id: str | None = Field(None)
    queue_url: str | None = Field(None)
    resource_group: str | None = Field(None)
    subscription_id: str | None = Field(None)


class ExternalLocationEffectiveFileEventQueueProvidedPubsub(BaseModel):
    managed_resource_id: str | None = Field(None)
    subscription_name: str | None = Field(None)


class ExternalLocationEffectiveFileEventQueueProvidedSqs(BaseModel):
    managed_resource_id: str | None = Field(None)
    queue_url: str | None = Field(None)


class ExternalLocationEffectiveFileEventQueue(BaseModel):
    managed_aqs: ExternalLocationEffectiveFileEventQueueManagedAqs | None = Field(None)
    managed_pubsub: ExternalLocationEffectiveFileEventQueueManagedPubsub | None = Field(
        None
    )
    managed_sqs: ExternalLocationEffectiveFileEventQueueManagedSqs | None = Field(None)
    provided_aqs: ExternalLocationEffectiveFileEventQueueProvidedAqs | None = Field(
        None
    )
    provided_pubsub: ExternalLocationEffectiveFileEventQueueProvidedPubsub | None = (
        Field(None)
    )
    provided_sqs: ExternalLocationEffectiveFileEventQueueProvidedSqs | None = Field(
        None
    )


class ExternalLocationEncryptionDetailsSseEncryptionDetails(BaseModel):
    algorithm: str | None = Field(None)
    aws_kms_key_arn: str | None = Field(None)


class ExternalLocationEncryptionDetails(BaseModel):
    sse_encryption_details: (
        ExternalLocationEncryptionDetailsSseEncryptionDetails | None
    ) = Field(None)


class ExternalLocationFileEventQueueManagedAqs(BaseModel):
    queue_url: str | None = Field(None)
    resource_group: str = Field(...)
    subscription_id: str = Field(...)


class ExternalLocationFileEventQueueManagedPubsub(BaseModel):
    subscription_name: str | None = Field(None)


class ExternalLocationFileEventQueueManagedSqs(BaseModel):
    queue_url: str | None = Field(None)


class ExternalLocationFileEventQueueProvidedAqs(BaseModel):
    queue_url: str = Field(...)
    resource_group: str | None = Field(None)
    subscription_id: str | None = Field(None)


class ExternalLocationFileEventQueueProvidedPubsub(BaseModel):
    subscription_name: str = Field(...)


class ExternalLocationFileEventQueueProvidedSqs(BaseModel):
    queue_url: str = Field(...)


class ExternalLocationFileEventQueue(BaseModel):
    managed_aqs: ExternalLocationFileEventQueueManagedAqs | None = Field(None)
    managed_pubsub: ExternalLocationFileEventQueueManagedPubsub | None = Field(None)
    managed_sqs: ExternalLocationFileEventQueueManagedSqs | None = Field(None)
    provided_aqs: ExternalLocationFileEventQueueProvidedAqs | None = Field(None)
    provided_pubsub: ExternalLocationFileEventQueueProvidedPubsub | None = Field(None)
    provided_sqs: ExternalLocationFileEventQueueProvidedSqs | None = Field(None)


class ExternalLocationBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_external_location`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    credential_name: str = Field(...)
    name: str = Field(...)
    url: str = Field(...)
    comment: str | None = Field(None)
    enable_file_events: bool | None = Field(None)
    fallback: bool | None = Field(None)
    force_destroy: bool | None = Field(None)
    force_update: bool | None = Field(None)
    isolation_mode: str | None = Field(None)
    metastore_id: str | None = Field(None)
    owner: str | None = Field(None)
    read_only: bool | None = Field(None)
    skip_validation: bool | None = Field(None)
    effective_file_event_queue: ExternalLocationEffectiveFileEventQueue | None = Field(
        None
    )
    encryption_details: ExternalLocationEncryptionDetails | None = Field(None)
    file_event_queue: ExternalLocationFileEventQueue | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_external_location"


__all__ = [
    "ExternalLocationEffectiveFileEventQueue",
    "ExternalLocationEffectiveFileEventQueueManagedAqs",
    "ExternalLocationEffectiveFileEventQueueManagedPubsub",
    "ExternalLocationEffectiveFileEventQueueManagedSqs",
    "ExternalLocationEffectiveFileEventQueueProvidedAqs",
    "ExternalLocationEffectiveFileEventQueueProvidedPubsub",
    "ExternalLocationEffectiveFileEventQueueProvidedSqs",
    "ExternalLocationEncryptionDetails",
    "ExternalLocationEncryptionDetailsSseEncryptionDetails",
    "ExternalLocationFileEventQueue",
    "ExternalLocationFileEventQueueManagedAqs",
    "ExternalLocationFileEventQueueManagedPubsub",
    "ExternalLocationFileEventQueueManagedSqs",
    "ExternalLocationFileEventQueueProvidedAqs",
    "ExternalLocationFileEventQueueProvidedPubsub",
    "ExternalLocationFileEventQueueProvidedSqs",
    "ExternalLocationBase",
]
