from pydantic import Field

from laktory.models.resources.databricks.accessrequestdestinations import (
    AccessRequestDestinations,
)
from laktory.models.resources.databricks.accessrequestdestinations_base import (
    AccessRequestDestinationsDestinations,
)


class AccessRequestDestinationsMixin:
    """
    Shared support for Unity Catalog securables (catalog, schema, table,
    volume) declaring where their access requests are sent with an
    `access_request_destinations` field.
    """

    access_request_destinations: list[AccessRequestDestinationsDestinations] = Field(
        None,
        description="Destinations (email, Slack, Microsoft Teams, webhook or the securable owner) notified when users request access to this securable. Deployed as an `AccessRequestDestinations` resource.",
    )

    def get_access_request_destinations_additional_resources(
        self, securable_type: str
    ) -> list:
        if not self.access_request_destinations:
            return []
        return [
            AccessRequestDestinations(
                securable={
                    "type": securable_type,
                    "full_name": f"${{resources.{self.resource_name}.id}}",
                },
                destinations=self.access_request_destinations,
            )
        ]
