from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.share_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.share_base import ShareBase

#
# __all__ = [
#     "Share",
#     "ShareObject",
#     "ShareObjectPartition",
#     "ShareObjectPartitionValue",
# ]


class ShareProviderConfig(BaseModel):
    """Provider Config"""

    workspace_id: str = Field(
        ...,
        description="Workspace ID which the resource belongs to. This workspace must be part of the account which the provider is configured with.",
    )


class Share(ShareBase):
    """
    Databricks Share for Delta Sharing

    A share is a container that holds the objects to be shared with recipients.
    Shares enable data sharing between Databricks workspaces.

    Examples
    --------
    ```py
    ```
    """

    provider_config: ShareProviderConfig = Field(None, description="Provider config")
